import streamlit as st
import pandas as pd
import io
from datetime import datetime
import time
import requests
import json
import re

# Configuração da página
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

def log_message(level, message):
    if 'log_messages' not in st.session_state:
        st.session_state.log_messages = []
    st.session_state.log_messages.append({'level': level, 'content': message, 'time': datetime.now()})

# --- Funções Auxiliares (sem alterações na lógica interna) ---
@st.cache_data(ttl=600)
def obter_token_thorpe_ex_cached():
    try:
        response = requests.post(TOKEN_URL_THORPE_EX, headers=TOKEN_HEADERS_THORPE, json=AUTH_PAYLOAD_THORPE_EX, timeout=15)
        response.raise_for_status()
        log_message('info', 'Token Thorpe-EX obtido com sucesso.')
        return response.json().get('token')
    except requests.exceptions.RequestException as e:
        log_message('error', f"(API Thorpe-EX) Erro ao obter token: {e}"); return None

@st.cache_data(ttl=600)
def obter_token_thorpe_es_cached():
    try:
        response = requests.post(TOKEN_URL_THORPE_ES, headers=TOKEN_HEADERS_THORPE, json=AUTH_PAYLOAD_THORPE_ES, timeout=15)
        response.raise_for_status()
        log_message('info', 'Token Thorpe-ES obtido com sucesso.')
        return response.json().get('token')
    except requests.exceptions.RequestException as e:
        log_message('error', f"(API Thorpe-ES) Erro ao obter token: {e}"); return None

def consultar_pedido_thorpe(token, url_base, pedido_raw, origem):
    if not token: return None
    url_pedido = f"{url_base}{pedido_raw}"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    try:
        response = requests.get(url_pedido, headers=headers, timeout=15)
        response.raise_for_status(); return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 404: log_message('warning', f"(API Thorpe-{origem}) Erro HTTP em pedido {pedido_raw}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        log_message('warning', f"(API Thorpe-{origem}) Erro de requisição em pedido {pedido_raw}: {e}")
        return None

def extrair_status_recente_thorpe_generico(dados_pedido_api, id_pedido_raw):
    if not dados_pedido_api: return {"pedido_raw_key": str(id_pedido_raw), "Status Thorpe": "---", "Data Status Thorpe": pd.NaT}
    eventos = []
    status_atual = dados_pedido_api.get("statusAtual")
    if not status_atual:
        status_atual = "---"

    for item in dados_pedido_api.get("historicoStatus", []):
        if item.get("data") and item.get("status"):
            try:
                eventos.append({'date': pd.to_datetime(item["data"]), 'status': item["status"]})
            except (ValueError, TypeError):
                pass
    for item in dados_pedido_api.get("informacoesRastreio", {}).get("rastreio", []):
        if item.get("dataHora") and item.get("status"):
            try:
                eventos.append({'date': pd.to_datetime(item["dataHora"]), 'status': item["status"]})
            except (ValueError, TypeError):
                pass

    if eventos:
        eventos.sort(key=lambda x: x['date'], reverse=True)
        latest_event = eventos[0]
        return {
            "pedido_raw_key": str(id_pedido_raw),
            "Status Thorpe": latest_event.get('status', '---'),
            "Data Status Thorpe": latest_event.get('date', pd.NaT)
        }
    
    return {"pedido_raw_key": str(id_pedido_raw), "Status Thorpe": dados_pedido_api.get("statusAtual", "---"), "Data Status Thorpe": pd.NaT}

def buscar_dados_thorpe_combinado_api(lista_pedidos_para_thorpe: list, token_ex, token_es, placeholder):
    if not lista_pedidos_para_thorpe: return pd.DataFrame()
    total = len(lista_pedidos_para_thorpe)
    all_api_data = []
    progress_bar = placeholder.progress(0, text=f"Consultando {total} pedidos na API Thorpe...")
    for i, pedido_id in enumerate(lista_pedidos_para_thorpe):
        dados = None
        if token_ex:
            dados = consultar_pedido_thorpe(token_ex, API_PEDIDOS_BASE_URL_THORPE_EX, pedido_id, "EX")
        if not dados and token_es:
            dados = consultar_pedido_thorpe(token_es, API_PEDIDOS_BASE_URL_THORPE_ES, pedido_id, "ES")
        
        info = extrair_status_recente_thorpe_generico(dados, pedido_id)
        all_api_data.append(info)
        time.sleep(0.05)
        progress_bar.progress((i + 1) / total, text=f"Consultando {total} pedidos na API Thorpe...")
    progress_bar.progress(1.0, text="Consulta Thorpe concluída!")
    return pd.DataFrame(all_api_data)

def preparar_id_para_bd(pedido_id_excel):
    id_str = str(pedido_id_excel).replace('_CANC', '')
    return id_str[:-2] if len(id_str) == 11 and id_str.isnumeric() else id_str

def buscar_dados_api(url, lista_ids, placeholder, nome_api):
    if not lista_ids: return pd.DataFrame()
    resultados = []
    total = len(lista_ids)
    progress_bar = placeholder.progress(0, text=f"Consultando {total} em {nome_api}...")
    for i, pedido_id in enumerate(lista_ids):
        try:
            response = requests.post(url, headers=API_CONTROLADORIA_HEADERS, json={"pedido": pedido_id}, timeout=20)
            response.raise_for_status()
            dados = response.json()
            if dados: resultados.extend(dados)
            time.sleep(0.1)
        except requests.exceptions.RequestException as e:
            log_message('warning', f"Erro na API {nome_api} para o pedido {pedido_id}: {e}")
            continue
        progress_bar.progress((i + 1) / total, text=f"Consultando {total} em {nome_api}...")
    progress_bar.progress(1.0, text=f"Consulta {nome_api} concluída!")
    if not resultados: log_message('info', f"(API {nome_api}) Nenhum registro encontrado para os pedidos consultados.")
    else: log_message('info', f"(API {nome_api}) {len(resultados)} registros recebidos.")
    return pd.DataFrame(resultados)

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
    df_export = df_consolidado.copy()
    df_export['Auditoria'] = df_export['pedido_normalizado'].map(audit_map).fillna('OK')
    if not df_crm.empty:
        df_crm_copy = df_crm.copy()
        df_crm_copy['datahora_andamento'] = pd.to_datetime(df_crm_copy['datahora_andamento'], errors='coerce')
        df_crm_recente = df_crm_copy.sort_values('datahora_andamento', ascending=False).drop_duplicates('pedido_raw', keep='first')
        df_crm_recente = df_crm_recente[['pedido_raw', 'andamento_obs', 'usuario_andamento', 'datahora_andamento']].rename(columns={
            'andamento_obs': 'Última Obs. CRM', 'usuario_andamento': 'Último Usuário CRM', 'datahora_andamento': 'Data Último And. CRM'
        })
        df_export = pd.merge(df_export, df_crm_recente, on='pedido_raw', how='left')
    for col in df_export.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        df_export[col] = df_export[col].dt.strftime('%d/%m/%Y %H:%M:%S')
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Detalhes_Nota_a_Nota')
    return output.getvalue()

# --- Interface e Lógica Principal ---
st.title("Consulta Massiva de Pedidos - Controladoria")

st.sidebar.header("Configurações da Consulta")
st.sidebar.subheader("1. Escolha seu arquivo Excel:")
uploaded_file = st.sidebar.file_uploader("Escolha seu arquivo Excel:", type=["xlsx", "xls"], key="uploader", label_visibility="collapsed")

if 'dados_carregados' not in st.session_state:
    st.session_state.dados_carregados = False
    st.session_state.log_messages = []
    st.session_state.total_pedidos_input = 0

if uploaded_file and st.session_state.get('last_uploaded_file') != uploaded_file.name:
    st.session_state.dados_carregados = False
    st.session_state.last_uploaded_file = uploaded_file.name

coluna_selecionada = None
if uploaded_file:
    df_excel = pd.read_excel(uploaded_file, engine='openpyxl' if uploaded_file.name.endswith('xlsx') else 'xlrd')
    colunas = df_excel.columns.tolist()
    default_ix = next((i for i, c in enumerate(colunas) if 'pedido' in c.lower()), 0)
    st.sidebar.subheader("2. Selecione a coluna dos pedidos:")
    coluna_selecionada = st.sidebar.selectbox("Selecione a coluna dos pedidos:", colunas, index=default_ix, label_visibility="collapsed")

st.sidebar.subheader("3. Iniciar Processo.")
process_button = st.sidebar.button("PROCESSAR CONSULTA", type="primary")

if process_button and uploaded_file and coluna_selecionada:
    st.session_state.dados_carregados = False
    st.session_state.log_messages = []
    
    placeholder_detalhes, placeholder_thorpe, placeholder_crm = st.empty(), st.empty(), st.empty()
    
    try:
        df_base = df_excel[[coluna_selecionada]].copy().rename(columns={coluna_selecionada: "ID Original Excel"})
        df_base.dropna(subset=["ID Original Excel"], inplace=True)
        def sanitize_id(pid):
            pid_str = str(pid).strip()
            if pid_str.endswith('.0'):
                return pid_str[:-2]
            return pid_str.upper()
        df_base["ID Original Excel"] = df_base["ID Original Excel"].apply(sanitize_id)
        df_base.drop_duplicates(subset=["ID Original Excel"], inplace=True, keep='first')
        df_base["ID_para_Consulta_API"] = df_base["ID Original Excel"].apply(preparar_id_para_bd)
        
        st.session_state.total_pedidos_input = len(df_base)
        log_message('info', f"Arquivo Excel lido: {st.session_state.total_pedidos_input} pedidos únicos para consulta.")

        if not df_base.empty:
            ids_limpos = df_base["ID_para_Consulta_API"].unique().tolist()
            df_detalhado = buscar_dados_api(API_PEDIDO_DETALHADO_URL, ids_limpos, placeholder_detalhes, "Pedidos Detalhados")
            
            if not df_detalhado.empty:
                map_norm_to_orig = pd.Series(df_base['ID Original Excel'].values, index=df_base['ID_para_Consulta_API']).to_dict()
                df_detalhado['ID Original Excel'] = df_detalhado['pedido_normalizado'].map(map_norm_to_orig)

                def decide_thorpe_id(row):
                    original_id = str(row['ID Original Excel'])
                    if len(original_id) == 11 and original_id.isnumeric():
                        return original_id
                    else:
                        return str(row['pedido_raw'])
                
                df_detalhado['ID_para_Thorpe'] = df_detalhado.apply(decide_thorpe_id, axis=1)
                
                lista_para_thorpe = df_detalhado['ID_para_Thorpe'].unique().tolist()
                token_ex, token_es = obter_token_thorpe_ex_cached(), obter_token_thorpe_es_cached()
                df_thorpe = buscar_dados_thorpe_combinado_api(lista_para_thorpe, token_ex, token_es, placeholder_thorpe)

                if not df_thorpe.empty:
                    df_detalhado = pd.merge(df_detalhado, df_thorpe, left_on='ID_para_Thorpe', right_on='pedido_raw_key', how='left').drop(columns=['pedido_raw_key', 'ID_para_Thorpe'])
                
                df_crm = buscar_dados_api(API_CRM_URL, ids_limpos, placeholder_crm, "CRM")
                
                st.session_state.df_consolidado = df_detalhado
                st.session_state.df_crm = df_crm
                st.session_state.dados_carregados = True
            else:
                log_message('error', "A consulta inicial não retornou resultados. O processamento foi interrompido.")
    except Exception as e:
        log_message('error', f"Ocorreu um erro geral no processamento: {e}")
        import traceback
        log_message('error', traceback.format_exc())

if st.session_state.dados_carregados:
    df_display_raw = st.session_state.df_consolidado.copy()
    df_crm_raw = st.session_state.df_crm.copy()

    # Lógica de pré-cálculo para os filtros de auditoria
    df_display_raw['valor_normalizado'] = pd.to_numeric(df_display_raw['valor_normalizado'], errors='coerce').fillna(0)
    resumo_valores = df_display_raw.groupby('pedido_normalizado')['valor_normalizado'].sum()
    validacoes_por_pedido = df_display_raw.groupby('pedido_normalizado')['validacao_pedido'].unique().apply(set)
    pedidos_com_nota = validacoes_por_pedido[validacoes_por_pedido != {'Pedido'}].index
    
    ids_caso1, ids_caso2, ids_caso3, ids_caso4, ids_caso5, ids_caso6 = [], [], [], [], [], []
    if not df_crm_raw.empty:
        df_crm_raw['datahora_andamento'] = pd.to_datetime(df_crm_raw['datahora_andamento'], errors='coerce')
        pedidos_positivos = resumo_valores[resumo_valores > 0].index
        pedidos_com_tratativa_real = df_crm_raw[df_crm_raw['andamento_descricao'] != 'EM EXPEDIÇÃO']['pedido_normalizado'].unique()
        ids_caso1 = [pid for pid in pedidos_positivos if pid not in pedidos_com_tratativa_real]

        df_crm_recente_por_raw = df_crm_raw.sort_values('datahora_andamento', ascending=False).drop_duplicates('pedido_raw', keep='first')
        regex_cancelamento = re.compile(r'^LIB.*CANC', re.IGNORECASE)
        cancelamentos_pendentes_raw = df_crm_recente_por_raw[df_crm_recente_por_raw['andamento_obs'].str.contains(regex_cancelamento, na=False, regex=True)]
        ids_caso2 = cancelamentos_pendentes_raw['pedido_normalizado'].unique()
        
        df_crm_recente_por_norm = df_crm_raw.sort_values('datahora_andamento', ascending=False).drop_duplicates('pedido_normalizado', keep='first')
        pedidos_finalizados_crm = df_crm_recente_por_norm[df_crm_recente_por_norm['andamento_descricao'].str.startswith('FINAL', na=False)]['pedido_normalizado'].unique()
        ids_caso5 = list(set(pedidos_com_nota) & set(pedidos_finalizados_crm))
        
        pedidos_em_andamento = list(set(pedidos_com_tratativa_real) - set(pedidos_finalizados_crm))
        ids_caso6 = list(set(pedidos_com_nota) & set(pedidos_em_andamento))

    pedidos_so_com_pedido = validacoes_por_pedido[validacoes_por_pedido == {'Pedido'}].index
    pedidos_bloqueados_T = df_display_raw[df_display_raw['bloqueada'] == 'T']['pedido_normalizado'].unique()
    ids_caso3 = list(set(pedidos_so_com_pedido) & set(pedidos_bloqueados_T))
    
    pedidos_valor_zero = resumo_valores[(resumo_valores > -1) & (resumo_valores < 1)].index
    ids_caso4 = list(set(pedidos_com_nota) & set(pedidos_valor_zero))
    
    final_audit_map = {}
    priority_order = [
        ('Pedidos com Cancelamento Pendente', ids_caso2), ('Pedidos Bloqueados sem Faturamento', ids_caso3),
        ('Pedidos Devolvidos', ids_caso4), ('Pedidos Finalizados', ids_caso5),
        ('Pedidos em Tratativa', ids_caso6), ('Pedidos sem Tratativa', ids_caso1)
    ]
    
    all_pids = df_display_raw['pedido_normalizado'].unique()
    for pid in all_pids:
        for name, id_list in priority_order:
            if pid in id_list:
                final_audit_map[pid] = name
                break
        if pid not in final_audit_map:
            final_audit_map[pid] = 'Outros Casos'

    counts = pd.Series(list(final_audit_map.values())).value_counts()

    # --- Interface ---
    total_pedidos = st.session_state.total_pedidos_input
    st.markdown(f"<h2 style='text-align: center;'>Visão Geral de {total_pedidos} Pedidos Computados</h2>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label="Pedidos sem Tratativa", value=counts.get('Pedidos sem Tratativa', 0))
    col2.metric(label="Cancelamento Pendente", value=counts.get('Pedidos com Cancelamento Pendente', 0))
    col3.metric(label="Pedidos Bloqueados sem Faturamento", value=counts.get('Pedidos Bloqueados sem Faturamento', 0))
    col4.metric(label="Pedidos Devolvidos", value=counts.get('Pedidos Devolvidos', 0))
    
    col5, col6, col7, col8 = st.columns(4) 
    col5.metric(label="Pedidos Finalizados", value=counts.get('Pedidos Finalizados', 0))
    col6.metric(label="Pedidos em Tratativa", value=counts.get('Pedidos em Tratativa', 0))
    col7.metric(label="Outros Casos", value=counts.get('Outros Casos', 0))

    with col8:
        sem_tratativa_count = counts.get('Pedidos sem Tratativa', 0)
        if total_pedidos > 0:
            indicador_controle = (total_pedidos - sem_tratativa_count) / total_pedidos
            st.metric(label="Indicador de Controle", value=f"{indicador_controle:.1%}")
        else:
            st.metric(label="Indicador de Controle", value="N/A")

    st.markdown("---")
    st.subheader("Casos para Auditoria")
    opcoes_auditoria = ['Todos'] + [name for name, _ in priority_order] + ['Outros Casos']
    filtro_auditoria = st.selectbox("Selecione um caso de auditoria:", options=opcoes_auditoria, label_visibility="collapsed")

    st.subheader("Filtros Gerais")
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1: filtro_pedido = st.text_input("Número do Pedido:")
    with fcol2: filtro_canal = st.selectbox("Canal de Venda:", options=['Todos'] + sorted(df_display_raw['canal_venda'].dropna().unique().tolist()))
    with fcol3: filtro_id_empresa = st.selectbox("ID Empresa:", options=['Todos'] + sorted(df_display_raw['id_empresa'].dropna().unique().tolist()))
    
    fcol4, fcol5, fcol6 = st.columns(3)
    with fcol4: filtro_status_thorpe = st.selectbox("Status Thorpe:", options=['Todos'] + sorted(df_display_raw['Status Thorpe'].dropna().unique().tolist()))
    with fcol5: filtro_motivo_bloqueio = st.selectbox("Motivo Bloqueio Pedido:", options=['Todos'] + sorted(df_display_raw['motivo_bloqueio'].dropna().unique().tolist()))
    with fcol6: filtro_transportadora = st.selectbox("Transportadora:", options=['Todos'] + sorted(df_display_raw['transportadora'].dropna().unique().tolist()))

    df_filtrado = df_display_raw.copy()
    if filtro_auditoria != 'Todos':
        pids_filtrados = [pid for pid, cat in final_audit_map.items() if cat == filtro_auditoria]
        df_filtrado = df_filtrado[df_filtrado['pedido_normalizado'].isin(pids_filtrados)]
    
    if filtro_pedido: df_filtrado = df_filtrado[df_filtrado['pedido_normalizado'].str.contains(filtro_pedido.upper(), na=False)]
    if filtro_canal != 'Todos': df_filtrado = df_filtrado[df_filtrado['canal_venda'] == filtro_canal]
    if filtro_id_empresa != 'Todos': df_filtrado = df_filtrado[df_filtrado['id_empresa'] == filtro_id_empresa]
    if filtro_status_thorpe != 'Todos': df_filtrado = df_filtrado[df_filtrado['Status Thorpe'] == filtro_status_thorpe]
    if filtro_motivo_bloqueio != 'Todos': df_filtrado = df_filtrado[df_filtrado['motivo_bloqueio'] == filtro_motivo_bloqueio]
    if filtro_transportadora != 'Todos': df_filtrado = df_filtrado[df_filtrado['transportadora'] == filtro_transportadora]
    
    st.markdown("---")
    
    st.subheader("Análise Detalhada por Pedido")
    if not df_filtrado.empty:
        tabela_resumo = df_filtrado.groupby('pedido_normalizado').agg(
            canal_venda=('canal_venda', 'max'), data_pedido=('data_pedido', 'max'),
            valor_liquido=('valor_normalizado', 'sum')
        ).reset_index()

        for _, row in tabela_resumo.iterrows():
            pedido_norm, valor_formatado = row['pedido_normalizado'], f"R$ {row['valor_liquido']:,.2f}"
            data_formatada = pd.to_datetime(row['data_pedido']).strftime('%d/%m/%Y') if pd.notna(row['data_pedido']) else 'N/A'
            categoria_auditoria = final_audit_map.get(pedido_norm, 'OK')
            prefixo_titulo = f"[{categoria_auditoria}] - " if categoria_auditoria != 'OK' else ""
            expander_title = f"#### {prefixo_titulo}{row['canal_venda']} | {pedido_norm} | {data_formatada} | {valor_formatado}"
            
            with st.expander(expander_title):
                st.markdown("<h6>Detalhes do Pedido</h6>", unsafe_allow_html=True)
                detalhes_pedido = df_filtrado[df_filtrado['pedido_normalizado'] == pedido_norm].copy()
                if 'data_emissao' in detalhes_pedido.columns and 'hora_emissao' in detalhes_pedido.columns:
                    data_str = pd.to_datetime(detalhes_pedido['data_emissao'], errors='coerce').dt.strftime('%d/%m/%Y')
                    hora_str = pd.to_datetime(detalhes_pedido['hora_emissao'], errors='coerce').dt.strftime('%H:%M:%S')
                    detalhes_pedido['Data/Hora Emissão'] = data_str.fillna('') + ' ' + hora_str.fillna('')
                    detalhes_pedido['Data/Hora Emissão'] = detalhes_pedido['Data/Hora Emissão'].str.strip().replace('', '---')
                ordem_final = ['validacao_pedido', 'canal_venda', 'filial', 'id_empresa', 'data_pedido','Data/Hora Emissão', 'valor_normalizado', 'uf_dest', 'transportadora','motivo_bloqueio', 'us_cadastro', 'tipo_nfe', 'nfe_cstat', 'data_expedicao','bloqueada', 'Status Thorpe', 'Data Status Thorpe']
                ordem_existente = [col for col in ordem_final if col in detalhes_pedido.columns]
                detalhes_display = detalhes_pedido[ordem_existente].fillna('---')
                
                # MUDANÇA: Formatar a data da Thorpe para incluir horas
                for col_data in ['data_pedido', 'data_expedicao']:
                     if col_data in detalhes_display.columns:
                        detalhes_display[col_data] = pd.to_datetime(detalhes_display[col_data], errors='coerce').dt.strftime('%d/%m/%Y')
                if 'Data Status Thorpe' in detalhes_display.columns:
                    detalhes_display['Data Status Thorpe'] = pd.to_datetime(detalhes_display['Data Status Thorpe'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M:%S')
                
                st.dataframe(detalhes_display, use_container_width=True, hide_index=True)

                st.markdown("<h6>Andamentos no CRM</h6>", unsafe_allow_html=True)
                detalhes_crm = df_crm_raw[df_crm_raw['pedido_normalizado'] == pedido_norm].copy().fillna('---')
                if not detalhes_crm.empty:
                    detalhes_crm['datahora_andamento'] = pd.to_datetime(detalhes_crm['datahora_andamento'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M:%S')
                    st.dataframe(detalhes_crm.sort_values(by='datahora_andamento', ascending=False), use_container_width=True, hide_index=True)
                else:
                    st.text("Nenhum andamento encontrado no CRM para este pedido.")
    else:
        st.info("Nenhum pedido corresponde aos filtros selecionados.")

    st.markdown("---")
    st.subheader("Exportar Resultados em .xlsx")
    df_export_base = df_display_raw.copy()
    if filtro_auditoria != 'Todos':
        pids_export = [pid for pid, cat in final_audit_map.items() if cat == filtro_auditoria]
        df_export_base = df_export_base[df_export_base['pedido_normalizado'].isin(pids_export)]
    if not df_export_base.empty:
        colE1, colE2 = st.columns(2)
        with colE1:
            resumo_export = df_export_base.groupby('pedido_normalizado').agg(canal_venda=('canal_venda', 'max'),data_pedido=('data_pedido', 'max'),valor_liquido=('valor_normalizado', 'sum')).reset_index()
            excel_resumido = gerar_excel_resumido(resumo_export, final_audit_map)
            st.download_button(label="📥 Exportar Resumo", data=excel_resumido, file_name=f"resumo_pedidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        with colE2:
            excel_detalhado = gerar_excel_detalhado(df_export_base, df_crm_raw, final_audit_map)
            st.download_button(label="📥 Exportar Detalhes", data=excel_detalhado, file_name=f"detalhes_pedidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    else:
        st.info("Nenhum dado para exportar com base no filtro de auditoria selecionado.")

if st.session_state.get('log_messages'):
    with st.expander("Ver Logs de Processamento", expanded=False):
        for log in st.session_state.log_messages:
            st.text(f"[{log['time'].strftime('%H:%M:%S')}] {log['level'].upper()}: {log['content']}")
