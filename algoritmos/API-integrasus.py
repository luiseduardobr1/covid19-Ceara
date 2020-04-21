import pandas as pd
import datetime
import numpy as np

# Tirar o aviso de usar .loc no pandas
pd.options.mode.chained_assignment = None

# Ler o arquivo csv
integra = pd.read_csv(
    'https://indicadores.integrasus.saude.ce.gov.br/api/casos-coronavirus/export-csv', encoding='latin-1')

# integra = pd.read_csv('casos_coronavirus (3).csv', encoding='latin-1')

# Mostrar todas as colunas
pd.set_option('display.max_columns', None)

# Organizar colunas que interessam para o estudo
analise = integra[['codigoPaciente', 'idSivep', 'idadePaciente', 'sexoPaciente', 'bairroPaciente', 'codigoMunicipioPaciente', 'municipioPaciente', 'estadoPaciente',
                   'paisPaciente', 'dataInicioSintomas', 'dataNotificacao', 'dataSolicitacaoExame', 'dataResultadoExame',
                   'resultadoFinalExame', 'classificacaoEstadoSivep', 'dataEntradaUtisSvep', 'dataInternacaoSivep',
                   'dataEvolucaoCasoSivep', 'dataSaidaUtisSvep', 'obitoConfirmado', 'dataObito', 'evolucaoCasoSivep']]


# Converter string data para datetime
analise['dataInicioSintomas'] = pd.to_datetime(
    analise['dataInicioSintomas'], errors='coerce')
analise['dataNotificacao'] = pd.to_datetime(
    analise['dataNotificacao'], errors='coerce')
analise['dataSolicitacaoExame'] = pd.to_datetime(
    analise['dataSolicitacaoExame'], errors='coerce')
analise['dataResultadoExame'] = pd.to_datetime(
    analise['dataResultadoExame'], errors='coerce')

# Comparar as datas com o dia de hoje para tirar incoerências
analise = analise[(analise['dataInicioSintomas'] <= pd.to_datetime(
    'today')) | (analise['dataInicioSintomas'].isnull())]
analise = analise[(analise['dataNotificacao'] <= pd.to_datetime(
    'today')) | (analise['dataNotificacao'].isnull())]
analise = analise[(analise['dataSolicitacaoExame'] <= pd.to_datetime(
    'today')) | (analise['dataSolicitacaoExame'].isnull())]
analise = analise[(analise['dataResultadoExame'] <= pd.to_datetime(
    'today')) | (analise['dataResultadoExame'].isnull())]


# Ok - checar
def positivos(analise):
    
    # Filtra os exames positivos
    analise = analise[(analise['resultadoFinalExame']) == 'Positivo']

    # Residentes no CE ou desconhecido
    analise = analise[(analise['estadoPaciente']=='CE') | (analise['estadoPaciente'].isnull())]
    analise = analise.reset_index()
    
    # Cria uma coluna de data nova para ver as que estao disponiveis
    analise['dataCovalesce'] = (analise['dataInicioSintomas'].combine_first(analise['dataSolicitacaoExame']).combine_first(analise['dataResultadoExame']
                                                                                                                          .combine_first(analise['dataNotificacao'])))
    # Pacientes únicos
    print(analise.codigoPaciente.nunique())
    
    # Organiza os municipios em ordem alfabetica (NaN fica no final) compara se há repetidos em ambas colunas paciente e municipio, se pega somente o primeiro
    analise = analise.sort_values('municipioPaciente', ascending=True).drop_duplicates(subset=['codigoPaciente','municipioPaciente'], keep='first')
    
    # Tira repetidos pacientes com municipios diferentes (geralmente Fortaleza x NaN)
    analise = analise.drop_duplicates(subset=['codigoPaciente'], keep='first')
    
    # Tira sem data convalesce
    #analise = analise[(analise['dataCovalesce'].isnull()==True)]
    
    # Descartando os casos que estao como negativo no classificacaoEstadoSivep
    #analise = analise[analise['classificacaoEstadoSivep']!='Negativo']
    print('Total de casos confirmados: '+ str(len(analise)))
    return(analise)

# Número total de municípios bate, mas não o somatório
def municipios_afetados(analise):
    analise=positivos(analise)
    analise['municipioPaciente'].fillna('Sem informação', inplace=True)
    analise = analise['municipioPaciente'].value_counts().rename_axis('Municípios').reset_index(name='Valores')
    n_municipios=len(analise)-1
    print('Número de municípios: ' + str(n_municipios))
    return(analise)

# Ok
def obitos(analise):
    
    # Filtra as informações vazias
    analise = analise[analise['obitoConfirmado'].isnull()==False]
    
    # Apaga os pacientes repetidos
    analise = analise.sort_values('obitoConfirmado', ascending=True).drop_duplicates(subset=['codigoPaciente', 'obitoConfirmado'], keep='last')
    
    # Extrai apenas o que tem como confirmado de óbito
    analise = analise[analise['obitoConfirmado']==True]
    
    print('Total de óbitos: ' + str(len(analise)))
    return(analise)

# Ok - Suspeitos = Em análise + null
def suspeitos(analise):
    analise = analise[(analise['resultadoFinalExame']=='Em Análise') | (analise['resultadoFinalExame'].isnull())]
    analise = analise.drop_duplicates(subset=['codigoPaciente'], keep='first')
    analise=analise[(analise['estadoPaciente']=='CE') | (analise['estadoPaciente'].isnull())]
    print('Total de casos suspeitos: '+ str(len(analise)))
    return(analise)

# Ok - Suspeitos = Em análise + null
def inconclusivos(analise):
    analise = analise[analise['resultadoFinalExame']=='Inconclusivo']
    analise = analise.drop_duplicates(subset=['codigoPaciente'], keep='last')
    print('Total de exames inconclusivos: '+ str(len(analise)))
    return(analise)

# BATE, mas forçando - não bate mais
def em_analise(analise):
    analise = analise[analise['resultadoFinalExame']=='Em Análise']
    analise = analise.drop_duplicates(subset=['codigoPaciente'], keep='first')
    # analise=analise[(analise['estadoPaciente']=='CE') | (analise['estadoPaciente'].isnull())]
    analise = analise[(analise['evolucaoCasoSivep']=='Ignorado') | (analise['evolucaoCasoSivep'].isnull())]
    analise = analise[(analise['classificacaoEstadoSivep']=='Em Análise') | (analise['classificacaoEstadoSivep']=='Positivo') | (analise['classificacaoEstadoSivep'].isnull())]
    print('Total de exames em análise: '+ str(len(analise)))
    return(analise)

# NÃO BATE COM DO INTEGRASUS - A LÓGICA AQUI ESTÁ ERRADA:
def exames_realizados(analise):
    # Tira os NaN dos resultados
    analise = analise[analise['resultadoFinalExame'].isnull()==False]
    analise['dataCovalesce'] = (analise['dataSolicitacaoExame'].combine_first(analise['dataResultadoExame']))

    #analise = analise[(analise['dataCovalesce'].isnull()==False) | ((analise['municipioPaciente'].isnull()==False))]
    
    #analise = analise.sort_values('municipioPaciente', ascending=True).drop_duplicates(subset=['codigoPaciente', 'resultadoFinalExame', 'dataCovalesce', 'municipioPaciente', 'dataInicioSintomas','dataNotificacao'], keep='first')
    analise = analise.sort_values('municipioPaciente', ascending=True).drop_duplicates(subset=['codigoPaciente', 'municipioPaciente'])
                                                                                              
    
    
    
    print('Total de exames realizados: '+ str(len(analise)))
    return(analise)

# BATE forçando
def negativos(analise):
    analise = analise[analise['resultadoFinalExame']=='Negativo']
    analise = analise[(analise['estadoPaciente']=='CE') | (analise['estadoPaciente'].isnull())]
    analise = analise.reset_index()
    analise['dataCovalesce'] = (analise['dataInicioSintomas'].combine_first(analise['dataSolicitacaoExame']).combine_first(analise['dataResultadoExame']
                                                                                                                          .combine_first(analise['dataNotificacao'])))

    # Pacientes únicos
    print(analise.codigoPaciente.nunique())
    # Organiza os municipios em ordem alfabetica (NaN fica no final) compara se há repetidos em ambas colunas paciente e municipio, se pega somente o primeiro
    analise = analise.sort_values('municipioPaciente', ascending=True).drop_duplicates(subset=['codigoPaciente','municipioPaciente'], keep='first')
    # Tira repetidos pacientes com municipios diferentes (geralmente Fortaleza x NaN)
    analise = analise.drop_duplicates(subset=['codigoPaciente'], keep='first')
    
    analise = analise[analise['dataCovalesce'].isnull()==False]
    # Descartando os casos que estao como negativo no classificacaoEstadoSivep
    #analise = analise[(analise['classificacaoEstadoSivep']=='Negativo') | (analise['classificacaoEstadoSivep'].isnull())]
    print('Total de exames negativos: '+ str(len(analise)))
    return(analise)

# Hospitalizados dos casos confirmados - considerando que tenha ao menos um número de idSivep (mais hospitalizados)
# Considerando apenas com as internacoes e entrada uti (menos hospitalizados)
def hospitalizados(analise):
    analise=positivos(analise)
    analise = analise[(analise['idSivep'].isnull()==False) |
                      (analise['dataInternacaoSivep'].isnull()==False) | (analise['dataEntradaUtisSvep'].isnull()==False) | 
                      (analise['dataSaidaUtisSvep'].isnull()==False)]
    print('Total de hospitalizados é: '+ str(len(analise)))
    return(analise)

# Curados que tiveram caso confirmado - provavelmente no hospital registrado
def curados(analise):
    #analise=positivos(analise)
    #
    analise = analise[(analise['evolucaoCasoSivep']=='Cura')]
    analise = analise[(analise['obitoConfirmado'].isnull()==True) | (analise['obitoConfirmado']==False)]
    analise = analise.sort_values('municipioPaciente', ascending=True).drop_duplicates(subset=['codigoPaciente','municipioPaciente'], keep='first')
    # Tira repetidos pacientes com municipios diferentes (geralmente Fortaleza x NaN)
    analise = analise.drop_duplicates(subset=['codigoPaciente'], keep='first')
    print('Total de curados provavelmente no hospital: '+ str(len(analise)))
    return(analise)


# Casos confirmados
confirmados = positivos(analise)

# Obitos
obitos_total = obitos(analise)

# Inconclusivo
dados_inconclusivos = inconclusivos(analise)


# Municípios afetados
municipios = municipios_afetados(analise)
print(municipios)

# Suspeitos
suspeitos_total = suspeitos(analise)

print('\n')

# Exames em análise
exames_em_analise = em_analise(analise)

# Exames realizados
exames = exames_realizados(analise)

# Exames negativos
casos_negativos = negativos(analise)
#obitos_total['idadePaciente'] = obitos_total['idadePaciente'].fillna(0)
#print(obitos_total['idadePaciente'].sum()/155)

# Casos hospitalizados
hospital = hospitalizados(analise)
cura = curados(analise)

