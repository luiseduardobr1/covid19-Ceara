# -*- coding: latin-1 -*-

import pandas as pd
import datetime
import requests
import base64
from github import Github
from github import InputGitTreeElement
import time
import schedule
from dateutil import tz
import numpy as np
import re


def job():
    print("executando")
    tic = time.time()
    
    # Data inicial e final dos dados em CSV
    # OBS: As vezes essas datas tem bug's que param o script
    #df = pd.read_json(r'https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/filtro-data')
    
    
    def enviar_github(repository, name_from, name_dest, commit_msg):
        # TOKEN da minha conta
        token = 'YOUR TOKEN HERE'
        g = Github(token)

        repo = g.get_user().get_repo(repository)

        # Acessar o README.md
        contents = repo.get_contents("README.md")
        raw_data = contents.decoded_content.decode('utf-8')

        # Regex completo para URL's
        URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

        # Atualizar três primeiros links no README.md
        resumoCE = re.findall(URL_REGEX, raw_data)[0]
        municipiosCE = re.findall(URL_REGEX, raw_data)[1]
        faixaetariaCE = re.findall(URL_REGEX, raw_data)[2]
        if name_from[:8]=='DadosCE_':
            raw_data = raw_data.replace(resumoCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:11]=='municipios_':
            raw_data = raw_data.replace(municipiosCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:5]=='Faixa':
            raw_data = raw_data.replace(faixaetariaCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)

        # Update Readme.md
        repo.update_file(contents.path, commit_msg, raw_data, contents.sha)

        file_list = [
            name_from,
        ]
        file_names = [
            name_dest
        ]

        commit_message = commit_msg
        master_ref = repo.get_git_ref('heads/master')
        master_sha = master_ref.object.sha
        base_tree = repo.get_git_tree(master_sha)
        element_list = list()
        for i, entry in enumerate(file_list):
            with open(entry, encoding = 'utf-16') as input_file:
                data = input_file.read()
            if entry.endswith('.png'):
                data = base64.b64encode(data)
            element = InputGitTreeElement(file_names[i], '100644', 'blob', data)
            element_list.append(element)
        tree = repo.create_git_tree(element_list, base_tree)
        parent = repo.get_git_commit(master_sha)
        commit = repo.create_git_commit(commit_message, tree, [parent])
        master_ref.edit(commit.sha)

    # Extrair Municípios
    def municipios(data_inicial, data_final):
        date = data_inicial
        writeHeader = True
        while date<=data_final:
            # Confirmados
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-por-municipio?data={}&tipo=Confirmado&idMunicipio='.format(date.date()))
            df = pd.read_json(response.text)
            if df.empty == False:
                print(date)
                df.insert(loc=0, column='Data', value=date.date())

                # Óbitos
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-por-municipio?data={}&tipo=Óbito&idMunicipio='.format(date.date()))
                dff = pd.read_json(response.text)
                #dff.insert(loc=0, column='Data', value=date.date())

                if dff.empty==False:

                    # Juntar dataframes de casos confirmados e óbitos considerando a possibilidade
                    # de existir óbito em algum município sem nenhum caso confirmado
                    df = pd.merge(df, dff, how='outer', on='idMunicipio')
                    df['municipio_x'] = np.where(pd.isna(df['municipio_x']), df['municipio_y'], df['municipio_x'])
                    df.drop(columns=['municipio_y', 'tipo_y'], inplace=True)
                    df['tipo_x'].fillna('Registrado somente óbito', inplace=True)
                    df['Data'].fillna(date.date(), inplace=True)

                    # Organizando o dataframe resultante
                    df.fillna(0, inplace=True)
                    df['idMunicipio'] = df['idMunicipio'].astype(int)
                    df['qtdObito'] = df['qtdObito'].astype(int)
                    df['qtdConfirmado'] = df['qtdConfirmado'].astype(int)
                    df.rename(columns={"municipio_x":"municipio","tipo_x": "tipo", "qtdObito": "Obitos"}, inplace=True)
                    df.sort_values(by=['municipio'], inplace=True)

                else:
                    df['Obitos']=0

                # Salva como um csv com header
                filename = r'municipios_'+data_final.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
                if writeHeader is True:
                    df.to_csv(filename, mode='a', encoding='utf-16', index = None)
                    writeHeader = False
                else:
                    df.to_csv(filename, mode='a', encoding='utf-16', index = None, header=False)

            # +1 dia a cada loop
            date += datetime.timedelta(days=1)

        # Envia CSV para meu repositorio covid19-Ceara no github 
        path = 'integraSUS_Municipios_CE/'+filename
        enviar_github('covid19-Ceara', filename, path, 'Atualizado em: ' + data_final.strftime('%d/%m/%Y %H:%M:%S'))

    def resultados_exames(data_inicial, data_final):
        date = data_inicial
        writeHeader = True
        while date<=data_final:
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-por-tipo?data={}&tipo=Confirmado&idMunicipio='.format(date))
            df = pd.read_json(response.text)

            if df.empty==False:
                # Municipios Afetados
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-municipios-casos-confirmados?data={}&tipo=Confirmado&idMunicipio='.format(date))
                dfmun = pd.read_json(response.text)

                #Óbitos
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-obitos?data={}&tipo=Confirmado&idMunicipio='.format(date))
                dfmun2 = pd.read_json(response.text)

                #Suspeitos
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-suspeitos?data={}&tipo=Confirmado&idMunicipio='.format(date))
                dfmun3 = pd.read_json(response.text)

                # Transposta e alterar nome
                df = df.set_index('tipo').transpose()
                df_new = df.rename(index={'quantidade': date.date()})

                if not set(['Data', 'Em Análise', 'Inconclusivo', 'Negativo', 'Positivo']).issubset(df_new.columns):
                    df_new.insert(loc=0, column='Data', value=date.date())  

                    if not {'Em Análise'}.issubset(df_new.columns):
                        df_new.insert(loc=1, column='Em Análise', value=0)
                    if not {'Inconclusivo'}.issubset(df_new.columns):
                        df_new.insert(loc=2, column='Inconclusivo', value=0)
                    if not {'Negativo'}.issubset(df_new.columns):
                        df_new.insert(loc=3, column='Negativo', value=0)
                    if not {'Positivo'}.issubset(df_new.columns):
                        df_new.insert(loc=4, column='Positivo', value=0)

                    df_new.insert(loc=5, column='Municios Afetados', value=dfmun.loc[0, 'quantidade']) 
                    df_new.insert(loc=6, column='Óbitos', value=dfmun2.loc[0, 'quantidade']) 
                    df_new.insert(loc=1, column='Suspeitos', value=dfmun3.loc[0, 'quantidade'])

                # Calcular a taxa de letalidade (%)
                try:
                    df_new['Taxa de letalidade (%)'] = round(100*df_new['Óbitos']/df_new['Positivo'], 3)
                except ZeroDivisionError:
                    df_new['Taxa de letalidade (%)']=0

                # Adiciona uma coluna estado = CE
                df_new['Estado'] = 'CE'

                # Salva como um csv com header
                filename = r'DadosCE_'+data_final.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
                if writeHeader is True:
                    df_new.to_csv(filename, mode='a', encoding='utf-16', index = None)
                    writeHeader = False
                else:
                    df_new.to_csv(filename, mode='a', encoding='utf-16', index = None, header=False)

            # +1 dia a cada loop
            date += datetime.timedelta(days=1) 

        # Envia CSV para meu repositorio covid19-Ceara no github    
        enviar_github('covid19-Ceara', filename, 'integraSUS_Resumo_CE/'+filename, 'Atualizado em: ' + data_final.strftime('%d/%m/%Y %H:%M:%S'))

    def faixa_etaria(data_inicial, data_final):
        writeHeader = True
        date = data_inicial
        while date<=data_final:
            teste = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-por-faixa-etaria-sexo?data={}&tipo=Confirmado&idMunicipio='.format(date.date()))
            if teste.empty==False:
                teste.insert(loc=0, column='Data', value=date.date())
                teste = teste.set_index('Data')
                teste = teste.drop(columns=['tipo'])
                teste = teste.fillna('Indefinido')

                filename = r'Faixa_Etaria_'+data_final.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
                if writeHeader is True:
                        teste.to_csv(filename, mode='a', encoding='utf-16')
                        writeHeader = False
                else:
                    teste.to_csv(filename, mode='a', encoding='utf-16', header=False)

            date += datetime.timedelta(days=1) 
        enviar_github('covid19-Ceara', filename, 'integraSUS_Faixa_Etaria_CE/'+filename, 'Atualizado em: ' + data_final.strftime('%d/%m/%Y %H:%M:%S'))

    municipios(data_inicial, data_final)
    resultados_exames(data_inicial, data_final)
    faixa_etaria(data_inicial, data_final)

    toc = time.time()
    get_time=round(toc-tic,3)
    print('Finished in ' + str(get_time) + ' seconds')
    


schedule.every(5).hours.do(job)

while True:
    data_inicial = datetime.datetime(2020, 3, 1, 0, 0, 0, 0)
    data_final = datetime.datetime.now() - datetime.timedelta(hours=3)
    
    schedule.run_pending()
    time.sleep(1)
