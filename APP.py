import streamlit as st
import pandas as pd
import io
from datetime import datetime
import requests
import re
import concurrent.futures

# --- Configuração da página ---
st.set_page_config(page_title="Consulta de Pedidos", page_icon="🔍", layout="wide")

# --- Configurações de API ---
TOKEN_URL_THORPE_EX = 'https://apiextrema.thorpe.com.br/v2/token'
API_PEDIDOS_BASE_URL_THORPE_EX = 'http://apiextrema.thorpe.com.br/v2/pedidos/'
AUTH_PAYLOAD_THORPE_EX = { "usuario": "intengage", "senha": "QE2S0w2o" }
TOKEN_HEADERS_THORPE = { 'Content-Type': 'application/json' }
TOKEN_URL_THORPE_ES = 'https://apies.thorpe.com.br/v2/token'
API_PEDIDOS_BASE_URL_THORPE_ES = 'https://apies.thorpe.com.br/v2/pedidos/'
AUTH_PAYLOAD_THORPE_ES = { "usuario": "intengagees", "senha": "QE2S0w2o" }
API_PEDIDO_DETALHADO_URL = "http://209.14.71.180:3000/controladoria-pedido-detalhado"
API_CRM_URL = "http://209.14.71.180:3000/controle-andamento-crm"
API_CONTROLADORIA_HEADERS = {
    'Authorization': 'Bearer engage@secure2024',
    'Content-Type': 'application/json'
}

# --- Funções de Logging e Cache ---
def log_message(level, message):
    if 'log_messages' not in st.session_state:
        st.session_state.log_messages = []
    st.session_state.log_messages.append({'level': level, 'content': message, 'time': datetime.now()})

@st.cache_data(ttl=600)
def obter_token_thorpe_ex_cached():
    try:
        response = requests.post(TOKEN_URL_THORPE_EX, headers=TOKEN_HEADERS_THORPE, json=AUTH_PAYLOAD_THORPE_EX, timeout=15)
        response.raise_for_status()
        return response.json().get('token')
    except requests.exceptions.RequestException as e:
        log_message('error', f"(API Thorpe-EX) Erro ao obter token: {e}"); return None

@st.cache_data(ttl=600)
def obter_token_thorpe_es_cached():
    try:
        response = requests.post(TOKEN_URL_THORPE_ES, headers=TOKEN_HEADERS_THORPE, json=AUTH_PAYLOAD_THORPE_ES, timeout=15)
        response.raise_for_status()
        return response.json().get('token')
    except requests.exceptions.RequestException as e:
        log_message('error', f"(API Thorpe-ES) Erro ao obter token: {e}"); return None

# --- FUNÇÕES DE API OTIMIZADAS PARA EXECUÇÃO PARALELA ---
def consultar_api_sysemp(url, lista_ids, nome_api, max_workers=2):
    if not lista_ids: return pd.DataFrame()
    resultados = []
    
    def fetch_single(pedido_id, session):
        try:
            response = session.post(url, headers=API_CONTROLADORIA_HEADERS, json={"pedido": pedido_id}, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        with requests.Session() as session:
            future_to_pedido = {executor.submit(fetch_single, pedido_id, session): pedido_id for pedido_id in lista_ids}
            for future in concurrent.futures.as_completed(future_to_pedido):
                dados = future.result()
                if dados: resultados.extend(dados)
    
    log_message('info', f"(API {nome_api}) {len(resultados)} registros recebidos de {len(lista_ids)} pedidos consultados.")
    return pd.DataFrame(resultados)

def consultar_api_thorpe(lista_pedidos, token_ex, token_es, max_workers=6):
    if not lista_pedidos: return pd.DataFrame()
    all_api_data = []

    def fetch_single_thorpe(pedido_id):
        def _consultar(token, url_base, pid):
            if not token: return None
            url_pedido = f"{url_base}{pid}"
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            try:
                response = requests.get(url_pedido, headers=headers, timeout=20)
                response.raise_for_status(); return response.json()
            except requests.exceptions.RequestException: return None
        
        dados_pedido = _consultar(token_ex, API_PEDIDOS_BASE_URL_THORPE_EX, pedido_id)
        if not dados_pedido: dados_pedido = _consultar(token_es, API_PEDIDOS_BASE_URL_THORPE_ES, pedido_id)

        if not dados_pedido: return {"pedido_raw_key": str(pedido_id), "Status Thorpe": "---", "Data Status Thorpe": pd.NaT}
        eventos, status_atual = [], dados_pedido.get("statusAtual", "---")
        for item in dados_pedido.get("historicoStatus", []) + dados_pedido.get("informacoesRastreio", {}).get("rastreio", []):
            data_evento, status_evento = item.get("data") or item.get("dataHora"), item.get("status")
            if data_evento and status_evento:
                try: eventos.append({'date': pd.to_datetime(data_evento), 'status': status_evento})
                except (ValueError, TypeError): pass
        if eventos:
            eventos.sort(key=lambda x: x['date'], reverse=True)
            return {"pedido_raw_key": str(pedido_id), "Status Thorpe": eventos[0]['status'], "Data Status Thorpe": eventos[0]['date']}
        return {"pedido_raw_key": str(pedido_id), "Status Thorpe": status_atual, "Data Status Thorpe": pd.NaT}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pedido = {executor.submit(fetch_single_thorpe, pid): pid for pid in lista_pedidos}
        for future in concurrent.futures.as_completed(future_to_pedido):
            all_api_data.append(future.result())
    return pd.DataFrame(all_api_data)

# --- Funções de Geração de Excel (sem alteração) ---
@st.cache_data
def gerar_excel_resumido(df_resumo, audit_map):
    df_export = df_resumo.copy()
    df_export['Auditoria'] = df_export['pedido_normalizado'].map(audit_map).fillna('OK')
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Resumo_por_Pedido')
    return output.getvalue()

@st.cache_data
def gerar_excel_detalhado(df_consolidado, df_crm, audit_map):
    df_export, df_crm_copy = df_consolidado.copy(), df_crm.copy()
    df_export['Auditoria'] = df_export['pedido_normalizado'].map(audit_map).fillna('OK')
    if not df_crm_copy.empty:
        df_crm_copy['datahora_andamento'] = pd.to_datetime(df_crm_copy['datahora_andamento'], errors='coerce')
        df_crm_recente = df_crm_copy.sort_values('datahora_andamento', ascending=False).drop_duplicates('pedido_raw', keep='first')
        df_crm_recente = df_crm_recente[['pedido_raw', 'andamento_obs', 'usuario_andamento', 'datahora_andamento']].rename(columns={
            'andamento_obs': 'Última Obs. CRM', 'usuario_andamento': 'Último Usuário CRM', 'datahora_andamento': 'Data Último And. CRM'})
        df_export = pd.merge(df_export, df_crm_recente, on='pedido_raw', how='left')
    for col in df_export.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        df_export[col] = df_export[col].dt.strftime('%d/%m/%Y %H:%M:%S')
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Detalhes_Nota_a_Nota')
    return output.getvalue()

# --- Interface Principal do Streamlit ---
st.title("Consulta Massiva de Pedidos Cancelados - Controladoria")
st.sidebar.header("Configurações da Consulta")

if 'dados_carregados' not in st.session_state:
    st.session_state.dados_carregados = False
    st.session_state.log_messages = []
    st.session_state.total_pedidos_input = 0
    st.session_state.ids_nao_encontrados = []

uploaded_file = st.sidebar.file_uploader("1. Escolha seu arquivo Excel:", type=["xlsx", "xls"], key="uploader")

if uploaded_file:
    try:
        df_excel = pd.read_excel(uploaded_file, engine='openpyxl' if uploaded_file.name.endswith('xlsx') else 'xlrd')
        colunas = df_excel.columns.tolist()
        default_ix = next((i for i, c in enumerate(colunas) if 'pedido' in c.lower()), 0)
        coluna_selecionada = st.sidebar.selectbox("2. Selecione a coluna dos pedidos:", colunas, index=default_ix)
        
        if st.sidebar.button("3. PROCESSAR CONSULTA", type="primary"):
            st.session_state.dados_carregados, st.session_state.log_messages = False, []
            
            with st.spinner("Lendo e preparando os pedidos do arquivo..."):
                df_base = df_excel[[coluna_selecionada]].copy().rename(columns={coluna_selecionada: "ID Original Excel"})
                df_base.dropna(subset=["ID Original Excel"], inplace=True)
                df_base["ID Original Excel"] = df_base["ID Original Excel"].astype(str).str.strip().str.upper().str.replace(r'\.0$', '', regex=True)
                df_base.drop_duplicates(subset=["ID Original Excel"], inplace=True, keep='first')
                
                def preparar_id_para_bd(pid): return str(pid).replace('_CANC', '')
                df_base["ID_para_Consulta"] = df_base["ID Original Excel"].apply(preparar_id_para_bd)
                ids_limpos = df_base["ID_para_Consulta"].unique().tolist()
                st.session_state.total_pedidos_input = len(df_base)

            if not df_base.empty:
                with st.spinner(f"Consultando {len(ids_limpos)} pedidos em 3 APIs simultaneamente..."):
                    token_ex, token_es = obter_token_thorpe_ex_cached(), obter_token_thorpe_es_cached()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                        future_detalhado = executor.submit(consultar_api_sysemp, API_PEDIDO_DETALHADO_URL, ids_limpos, "Sysemp-Detalhado", max_workers=2)
                        future_crm = executor.submit(consultar_api_sysemp, API_CRM_URL, ids_limpos, "Sysemp-CRM", max_workers=2)
                        future_thorpe = executor.submit(consultar_api_thorpe, ids_limpos, token_ex, token_es, max_workers=6)
                        
                        df_detalhado = future_detalhado.result()
                        df_crm = future_crm.result()
                        df_thorpe = future_thorpe.result()
                
                st.success("Consulta às APIs concluída! Consolidando e analisando os dados...")

                if not df_detalhado.empty:
                    df_consolidado = pd.merge(df_detalhado, df_thorpe, left_on='pedido_normalizado', right_on='pedido_raw_key', how='left').drop(columns=['pedido_raw_key'])
                    st.session_state.df_consolidado = df_consolidado
                    st.session_state.df_crm = df_crm
                    st.session_state.dados_carregados = True
                else:
                    st.warning("A consulta principal (Sysemp-Detalhado) não retornou nenhum dado.")

                st.session_state.ids_nao_encontrados = list(set(ids_limpos) - set(df_detalhado['pedido_normalizado'].unique()))

    except Exception as e:
        st.error(f"Ocorreu um erro geral no processamento: {e}")
        import traceback; log_message('error', traceback.format_exc())

# ===== SEÇÃO DE EXIBIÇÃO E FILTROS =====
if st.session_state.get('dados_carregados'):
    df_display_raw = st.session_state.get('df_consolidado', pd.DataFrame())
    df_crm_raw = st.session_state.get('df_crm', pd.DataFrame())

    final_audit_map = {}
    
    # --- Lógica de Categorização ---
    df_display_raw['valor_normalizado'] = pd.to_numeric(df_display_raw['valor_normalizado'], errors='coerce').fillna(0)
    resumo_valores = df_display_raw.groupby('pedido_normalizado')['valor_normalizado'].sum()
    validacoes_por_pedido = df_display_raw.groupby('pedido_normalizado')['validacao_pedido'].unique().apply(set)
    pedidos_com_nota = validacoes_por_pedido[validacoes_por_pedido != {'Pedido'}].index

    ids_caso1, ids_caso2, ids_caso3, ids_caso4, ids_caso5 = [], [], [], [], []
    ids_cobranca_ativa, ids_carta_debito, ids_outras_tratativas, ids_faturamento_cancelado = [], [], [], []

    # --- LÓGICA CORRIGIDA PARA "PEDIDOS COM FATURAMENTO CANCELADO" ---
    # Garante que a lista exista para evitar NameError
    ids_faturamento_cancelado = []
    # 1. Isola apenas as linhas que são notas fiscais, ignorando o lançamento do "Pedido"
    df_notas_fiscais = df_display_raw[df_display_raw['validacao_pedido'] != 'Pedido'].copy()

    if not df_notas_fiscais.empty:
        # 2. Verifica, para cada nota, se ela está cancelada (por cstat ou sufixo)
        df_notas_fiscais['is_canceled'] = (df_notas_fiscais['nfe_cstat'] == '101') | (df_notas_fiscais['pedido_raw'].str.endswith('_CANC', na=False))
        
        # 3. Agrupa por pedido e verifica se TODAS as notas fiscais do grupo estão canceladas
        # O .all() garante que a condição 'is_canceled' seja verdadeira para todas as notas de um pedido
        pedidos_totalmente_cancelados_check = df_notas_fiscais.groupby('pedido_normalizado')['is_canceled'].all()
        
        # 4. Pega a lista de IDs onde a verificação acima foi True
        ids_faturamento_cancelado = pedidos_totalmente_cancelados_check[pedidos_totalmente_cancelados_check].index.tolist()
    # --- FIM DA LÓGICA CORRIGIDA ---


    if not df_crm_raw.empty:
        df_crm_raw['datahora_andamento'] = pd.to_datetime(df_crm_raw['datahora_andamento'], errors='coerce')
        pedidos_positivos = resumo_valores[resumo_valores > 0].index
        
        pedidos_com_tratativa_real = df_crm_raw[df_crm_raw['andamento_descricao'] != 'EM EXPEDIÇÃO']['pedido_normalizado'].unique()
        ids_caso1 = [pid for pid in pedidos_positivos if pid not in pedidos_com_tratativa_real and pid not in ids_faturamento_cancelado]
        
        df_crm_recente_por_raw = df_crm_raw.sort_values('datahora_andamento', ascending=False).drop_duplicates('pedido_raw', keep='first')
        regex_cancelamento = re.compile(r'^LIB.*CANC', re.IGNORECASE)
        cancelamentos_pendentes_raw = df_crm_recente_por_raw[df_crm_recente_por_raw['andamento_obs'].str.contains(regex_cancelamento, na=False, regex=True)]
        ids_caso2 = cancelamentos_pendentes_raw['pedido_normalizado'].unique().tolist()
        
        df_crm_recente_por_norm = df_crm_raw.sort_values('datahora_andamento', ascending=False).drop_duplicates('pedido_normalizado', keep='first')
        pedidos_finalizados_crm = df_crm_recente_por_norm[df_crm_recente_por_norm['andamento_descricao'].str.startswith('FINAL', na=False)]['pedido_normalizado'].unique()
        ids_caso5 = list(set(pedidos_com_nota) & set(pedidos_finalizados_crm))

        regex_cobranca = re.compile(r'JUR[ÍI]D|COBRAN[ÇC]|REVERSA.*?PAGAMENTO', re.IGNORECASE)
        pedidos_em_cobranca_crm = df_crm_raw[df_crm_raw['andamento_descricao'].str.contains(regex_cobranca, na=False)]['pedido_normalizado'].unique()
        ids_cobranca_ativa = list(set(pedidos_positivos) & set(pedidos_em_cobranca_crm))

        regex_carta_debito = re.compile(r'CARTA.*?D[ÉE]BITO', re.IGNORECASE)
        pedidos_carta_debito_crm = df_crm_raw[df_crm_raw['andamento_descricao'].str.contains(regex_carta_debito, na=False)]['pedido_normalizado'].unique()
        ids_carta_debito = list(set(pedidos_positivos) & set(pedidos_carta_debito_crm))
        
        pedidos_em_andamento = list(set(pedidos_com_tratativa_real) - set(pedidos_finalizados_crm))
        ids_tratativa_geral = list(set(pedidos_com_nota) & set(pedidos_em_andamento))
        ids_outras_tratativas = list(set(ids_tratativa_geral) - set(ids_cobranca_ativa) - set(ids_carta_debito) - set(ids_faturamento_cancelado))

    pedidos_so_com_pedido = validacoes_por_pedido[validacoes_por_pedido == {'Pedido'}].index
    pedidos_bloqueados_T = df_display_raw[df_display_raw['bloqueada'] == 'T']['pedido_normalizado'].unique()
    ids_caso3 = list(set(pedidos_so_com_pedido) & set(pedidos_bloqueados_T))

    pedidos_valor_zero = resumo_valores[(resumo_valores > -1) & (resumo_valores < 1)].index
    ids_caso4 = list(set(pedidos_com_nota) & set(pedidos_valor_zero) - set(ids_faturamento_cancelado))

    priority_order = [
        ('Pedidos com Cancelamento Pendente', ids_caso2),
        ('Pedidos Bloqueados sem Faturamento', ids_caso3),
        ('Pedidos com Faturamento Cancelado', ids_faturamento_cancelado),
        ('Pedidos Devolvidos', ids_caso4),
        ('Pedidos Finalizados', ids_caso5),
        ('Pedido com Cobrança Ativa', ids_cobranca_ativa),      
        ('Pedido com Carta de Débito', ids_carta_debito),      
        ('Pedidos em Outras Tratativas', ids_outras_tratativas), 
        ('Pedidos Pendentes de Tratativa', ids_caso1)
    ]
    
    for pid in df_display_raw['pedido_normalizado'].unique():
        for name, id_list in priority_order:
            if pid in id_list:
                final_audit_map[pid] = name; break
        if pid not in final_audit_map: final_audit_map[pid] = 'OK'
            
    counts = pd.Series(final_audit_map.values()).value_counts()

    # --- Exibição das Métricas (NOMES ORIGINAIS MANTIDOS) ---
    st.markdown(f"<h2 style='text-align: center;'>Visão Geral de {st.session_state.total_pedidos_input} Pedidos Computados</h2>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label="Pedidos Pendentes de Tratativa", value=counts.get('Pedidos Pendentes de Tratativa', 0))
    col2.metric(label="Cancelamento Pendente", value=counts.get('Pedidos com Cancelamento Pendente', 0))
    col3.metric(label="Pedidos Bloqueados sem Faturamento", value=counts.get('Pedidos Bloqueados sem Faturamento', 0))
    col4.metric(label="Pedidos Devolvidos", value=counts.get('Pedidos Devolvidos', 0))

    col5, col6, col7, col8 = st.columns(4)
    col5.metric(label="Pedido com Cobrança Ativa", value=counts.get('Pedido com Cobrança Ativa', 0))
    col6.metric(label="Pedido com Carta de Débito", value=counts.get('Pedido com Carta de Débito', 0))
    col7.metric(label="Pedidos em Outras Tratativas", value=counts.get('Pedidos em Outras Tratativas', 0))
    col8.metric(label="Pedidos Finalizados", value=counts.get('Pedidos Finalizados', 0))

    col9, col10, col11, _ = st.columns(4)
    col9.metric(label="Pedidos com Faturamento Cancelado", value=counts.get('Pedidos com Faturamento Cancelado', 0))
    col10.metric(label="Não Encontrados no Sysemp", value=len(st.session_state.ids_nao_encontrados))
    with col11:
        total_pedidos, sem_tratativa = st.session_state.total_pedidos_input, counts.get('Pedidos Pendentes de Tratativa', 0)
        st.metric(label="Indicador de Controle", value=f"{(total_pedidos - sem_tratativa) / total_pedidos:.1%}" if total_pedidos > 0 else "N/A")

    st.markdown("---")
    st.subheader("Casos para Auditoria")
    opcoes_auditoria = ['Todos'] + [name for name, _ in priority_order] + ['Não Encontrados no Sysemp']
    filtro_auditoria = st.selectbox("Selecione um caso de auditoria:", options=opcoes_auditoria, label_visibility="collapsed")
    
    # --- Lógica de Filtros e Exibição Detalhada ---
    df_filtrado_base = pd.DataFrame()
    if filtro_auditoria == 'Não Encontrados no Sysemp':
        if st.session_state.ids_nao_encontrados:
            st.info(f"Exibindo {len(st.session_state.ids_nao_encontrados)} pedidos não encontrados na base do Sysemp.")
            st.dataframe(pd.DataFrame(st.session_state.ids_nao_encontrados, columns=['ID do Pedido (Normalizado)']))
        else:
            st.success("✅ Todos os pedidos foram encontrados na base do Sysemp.")
    else:
        df_filtrado_base = df_display_raw.copy()
        if filtro_auditoria != 'Todos':
            pids_filtrados = [pid for pid, cat in final_audit_map.items() if cat == filtro_auditoria]
            df_filtrado_base = df_filtrado_base[df_filtrado_base['pedido_normalizado'].isin(pids_filtrados)]

    if not df_filtrado_base.empty:
        st.subheader("Filtros Gerais")
        fcol1, fcol2, fcol3 = st.columns(3)
        filtro_pedido = fcol1.text_input("Número do Pedido:")
        filtro_canal = fcol2.selectbox("Canal de Venda:", ['Todos'] + sorted(df_display_raw['canal_venda'].dropna().unique().tolist()))
        filtro_status_thorpe = fcol3.selectbox("Status Thorpe:", ['Todos'] + sorted(df_display_raw['Status Thorpe'].dropna().unique().tolist()))
        
        df_filtrado = df_filtrado_base.copy()
        if filtro_pedido: df_filtrado = df_filtrado[df_filtrado['pedido_normalizado'].str.contains(filtro_pedido.upper(), na=False)]
        if filtro_canal != 'Todos': df_filtrado = df_filtrado[df_filtrado['canal_venda'] == filtro_canal]
        if filtro_status_thorpe != 'Todos': df_filtrado = df_filtrado[df_filtrado['Status Thorpe'] == filtro_status_thorpe]

        st.markdown("---")
        st.subheader("Análise Detalhada por Pedido")
        if not df_filtrado.empty:
            tabela_resumo = df_filtrado.groupby('pedido_normalizado').agg(
                canal_venda=('canal_venda', 'first'),
                data_pedido=('data_pedido', 'first'),
                valor_liquido=('valor_normalizado', 'sum')
            ).reset_index()

            for _, row in tabela_resumo.iterrows():
                pedido_norm = row['pedido_normalizado']
                categoria_auditoria = final_audit_map.get(pedido_norm, 'OK')
                expander_title = f"[{categoria_auditoria}] Pedido: {pedido_norm} | Canal: {row['canal_venda']} | Valor: R$ {row['valor_liquido']:,.2f}"
                
                with st.expander(expander_title):
                    st.markdown("<h6>Detalhes do Pedido</h6>", unsafe_allow_html=True)
                    detalhes_pedido = df_filtrado[df_filtrado['pedido_normalizado'] == pedido_norm].copy()
                    st.dataframe(detalhes_pedido, use_container_width=True, hide_index=True)
                    
                    st.markdown("<h6>Andamentos no CRM</h6>", unsafe_allow_html=True)
                    detalhes_crm = df_crm_raw[df_crm_raw['pedido_normalizado'] == pedido_norm].copy()
                    if not detalhes_crm.empty:
                        detalhes_crm['datahora_andamento'] = pd.to_datetime(detalhes_crm['datahora_andamento'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M:%S')
                        st.dataframe(detalhes_crm.sort_values(by='datahora_andamento', ascending=False), use_container_width=True, hide_index=True)
                    else:
                        st.info("Nenhum andamento de CRM encontrado para este pedido.")
        else:
            st.info("Nenhum pedido corresponde aos filtros selecionados.")
            
        st.markdown("---")
        st.subheader("Exportar Resultados em .xlsx")
        colE1, colE2 = st.columns(2)
        excel_resumido = gerar_excel_resumido(df_filtrado.groupby('pedido_normalizado').agg(canal_venda=('canal_venda', 'max'),data_pedido=('data_pedido', 'max'),valor_liquido=('valor_normalizado', 'sum')).reset_index(), final_audit_map)
        colE1.download_button(label="📥 Exportar Resumo", data=excel_resumido, file_name=f"resumo_pedidos_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")
        
        excel_detalhado = gerar_excel_detalhado(df_filtrado, df_crm_raw, final_audit_map)
        colE2.download_button(label="📥 Exportar Detalhes", data=excel_detalhado, file_name=f"detalhes_pedidos_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx")

if st.session_state.get('log_messages'):
    with st.expander("Ver Logs de Processamento", expanded=False):
        # Exibe os logs mais recentes primeiro
        for log in reversed(st.session_state.log_messages):
            st.text(f"[{log['time'].strftime('%H:%M:%S')}] {log['level'].upper()}: {log['content']}")
