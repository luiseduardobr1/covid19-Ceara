# encoding: "ISO-8859-1"

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
        token = 'YOUR TOKEN'
        g = Github(token)

        repo = g.get_user().get_repo(repository)

        # Acessar o README.md
        contents = repo.get_contents("README.md")
        raw_data = contents.decoded_content.decode('utf-8')

        # Regex completo para URL's
        URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

        # Atualizar três primeiros links no README.md
        resumoCE = re.findall(URL_REGEX, raw_data)[0]
        municipiosCE = re.findall(URL_REGEX, raw_data)[1]
        faixaetariaCE = re.findall(URL_REGEX, raw_data)[2]
        obitosCE = re.findall(URL_REGEX, raw_data)[3]
        comorbidadesCE = re.findall(URL_REGEX, raw_data)[4]
        faixaetariaobitosCE = re.findall(URL_REGEX, raw_data)[5]


        if name_from[:8]=='DadosCE_':
            raw_data = raw_data.replace(resumoCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:11]=='municipios_':
            raw_data = raw_data.replace(municipiosCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:13]==r'Faixa_Etaria_':
            raw_data = raw_data.replace(faixaetariaCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:6]=='Obitos':
            raw_data = raw_data.replace(obitosCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:5]=='Comor':
            raw_data = raw_data.replace(comorbidadesCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)
        if name_from[:18]=='FaixaEtariaObitos_':
            raw_data = raw_data.replace(faixaetariaobitosCE,'https://github.com/luiseduardobr1/covid19-Ceara/blob/master/' + name_dest)

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
                #print(date)
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
                    ##### quantidade_y = óbitos
                    df['quantidade_y'] = df['quantidade_y'].astype(int)
                    df['quantidade_x'] = df['quantidade_x'].astype(int)
                    df.rename(columns={"municipio_x":"municipio","tipo_x": "tipo", "quantidade_y": "Obitos",
                                      "quantidade_x": "Confirmados"}, inplace=True)
                    df.rename(columns={"quantidade":"Confirmados"}, inplace=True)
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
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-confirmados?data={}&tipo=Confirmado&idMunicipio=&idRegiaoSaude=&idMacrorregiao='.format(date))
            df = pd.read_json(response.text)

            if df.empty==False:

                # Data do primeiro óbito
                try:
                    response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/limite-data?data=&idMunicipio=')
                    dfinicial = pd.DataFrame([pd.read_json(response.text, typ='series')])
                    data_primeiro_obito = dfinicial['dataMin'][0]
                    data_primeiro_obito = datetime.datetime.strptime(str(data_primeiro_obito), '%Y-%m-%d %H:%M:%S')
                except:
                    pass

                # Municipios Afetados
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-municipios-casos-confirmados?data={}&tipo=Confirmado&idMunicipio='.format(date))
                dfmun = pd.read_json(response.text)

                #Óbitos
                if date.date()>=data_primeiro_obito.date():
                    response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-obitos?data={}&tipo=Confirmado&idMunicipio='.format(date))
                    dfmun2 = pd.read_json(response.text)
                else:
                    dfmun2=pd.DataFrame([{"tipo": "óbito", "quantidade": "0"}])

                #Suspeitos
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-suspeitos?data={}&tipo=Confirmado&idMunicipio='.format(date))
                dfmun3 = pd.read_json(response.text)

                #Exames realizados
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-exames?data={}&tipo=Confirmado&idMunicipio=&idRegiaoSaude=&idMacrorregiao='.format(date))
                dfmun4 = pd.read_json(response.text)

                # Recuperados
                response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/coronavirus/qtd-recuperados?data={}&tipo=Confirmado&idMunicipio=&idRegiaoSaude=&idMacrorregiao='.format(date))
                dfmun5 = pd.read_json(response.text)

                # Transposta e alterar nome
                df = df.set_index('tipo').transpose()
                df_new = df.rename(index={'quantidade': date.date()})
                df_new.rename(columns={"Confirmado":"Positivo"}, inplace=True)


                if not set(['Data', 'Positivo']).issubset(df_new.columns):
                    df_new.insert(loc=0, column='Data', value=date.date())

                    if not {'Positivo'}.issubset(df_new.columns):
                        df_new.insert(loc=2, column='Positivo', value=0)

                    df_new.insert(loc=2, column='Municios Afetados', value=dfmun.loc[0, 'quantidade'])

                    # Corrigindo data do primeiro obito
                    if date<data_primeiro_obito:
                        df_new.insert(loc=3, column='Óbitos', value=0)
                    else:
                        df_new.insert(loc=3, column='Óbitos', value=dfmun2.loc[0, 'quantidade'])

                    # Inserando no dataframe df_new
                    df_new.insert(loc=1, column='Suspeitos', value=dfmun3.loc[0, 'quantidade'])
                    df_new.insert(loc=1, column='Exames realizados', value=dfmun4.loc[0, 'quantidade'])
                    df_new.insert(loc=5, column='Recuperados', value=dfmun5.loc[0, 'quantidade'])

                # Calcular a taxa de letalidade (%)
                try:
                    df_new['Taxa de letalidade (%)'] = round(100*df_new['Óbitos']/df_new['Positivo'], 3)
                except ZeroDivisionError:
                    df_new['Taxa de letalidade (%)']=0

                # Calcular a taxa de recuperação (%)
                try:
                    df_new['Taxa de recuperacao final (%)'] = round(100*df_new['Recuperados']/(df_new['Óbitos'] + df_new['Recuperados']), 3)
                    df_new['Taxa de óbito final (%)'] = 100 - df_new['Taxa de recuperacao final (%)']

                except ZeroDivisionError:
                    df_new['Taxa de recuperacao final (%)']=0
                    df_new['Taxa de óbito final (%)'] = 0

                # Adiciona uma coluna estado = CE
                df_new['Estado'] = 'CE'

                #Espaço vazio no título do dataframe
                df_new.columns = df_new.columns.fillna('Quantidade analisado')
                df_new = df_new.fillna(0)

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


    def hospital_vinci():
        # Data inicial e final
        datas = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/datas?dataInicio=&dataFim=&clinica=&sexo=&idMunicipio=')
        data_inicial = datas['dataMin'].loc[0]
        data_final = datas['dataMax'].loc[0]
        atualizado = datetime.datetime.now() - datetime.timedelta(hours=3)

        # Leitos, leitos ocupados e porcentagem ocupação
        ocupacao = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/qtd-leitos-ocupados?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        ocupacao['Taxa de Ocupacao (%)'] = round(100*ocupacao['leitosOcup']/ocupacao['qtdLeitos'],3)
        ocupacao.insert(loc=0, column='Data', value=data_final)

        # Tempo médio de internação
        tempo_medio = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/tempo-medio-internacao?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        ocupacao['Tempo medio internacao (dias)'] = tempo_medio['tmpMedioInternacao']

        # Óbitos
        obitos = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/qtd-obitos?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        ocupacao['Obitos'] = obitos['quantidade']

        # Altas
        altas = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/qtd-altas?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        ocupacao['Altas'] = altas['quantidade']

        # Quantidade por sexo
        sexo = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/qtd-por-sexo?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        ocupacao['Mulheres'] = sexo['quantidade'].loc[0] #Feminino
        ocupacao['Homens'] = sexo['quantidade'].loc[1] #Masculino
        ocupacao.to_csv('Ocupacao_Leitos_'+atualizado.strftime('%Y-%m-%d_%H_%M_%S')+'.csv', mode='a', encoding='utf-16', index=False)
        filename = 'Ocupacao_Leitos_'+atualizado.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
        enviar_github('covid19-Ceara', filename, 'Internacao_Leonardo_da_Vinci/'+filename, 'Atualizado em: ' + atualizado.strftime('%d/%m/%Y %H:%M:%S'))


        # Faixa etária
        faixaet = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/qtd-por-faixa-etaria-e-sexo?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        faixaet.to_csv('FaixaEtaria_Internados_'+atualizado.strftime('%Y-%m-%d_%H_%M_%S')+'.csv', mode='a', encoding='utf-16', index=False)
        filename = 'FaixaEtaria_Internados_'+atualizado.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
        enviar_github('covid19-Ceara', filename, 'Internacao_Leonardo_da_Vinci/'+filename, 'Atualizado em: ' + atualizado.strftime('%d/%m/%Y %H:%M:%S'))


        # Tipo de internação
        internacaotipo = pd.read_json('https://indicadores.integrasus.saude.ce.gov.br/api/acomp-internacoes/panorama-ocup-leitos?dataInicio={}&dataFim={}&clinica=&sexo=&idMunicipio='.format(data_inicial, data_final))
        internacaotipo.to_csv('Tipo_Internacao_'+atualizado.strftime('%Y-%m-%d_%H_%M_%S')+'.csv', mode='a', encoding='utf-16', index=False)
        filename = 'Tipo_Internacao_'+atualizado.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
        enviar_github('covid19-Ceara', filename, 'Internacao_Leonardo_da_Vinci/'+filename, 'Atualizado em: ' + atualizado.strftime('%d/%m/%Y %H:%M:%S'))

    def obitos_resumo(data_final):
        response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/limite-data?data=&idMunicipio=')
        dfinicial = pd.DataFrame([pd.read_json(response.text, typ='series')])
        date = dfinicial['dataMin'][0]
        date = datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        datainicialteste = datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        writeHeader = True
        while date<=data_final:
            # Qntd confirmados, obitos e letalidade
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/total?data={}&idMunicipio='.format(date.date()))
            df1 = pd.DataFrame([pd.read_json(response.text, typ='series')])
            df1.rename(columns={"quantidade":"Casos confirmados","quantidadeObitos": "Total obitos", "letalidade": "Letalidade (%)"}, inplace=True)
            df1['Letalidade (%)']=round(df1['Letalidade (%)'],3)

            # Media obitos por dia
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/media?data={}&idMunicipio='.format(date.date()))
            df2 = pd.DataFrame([pd.read_json(response.text, typ='series')])
            df1['Media Obitos por Dia'] = round(df2['mediaObitos'],3)

            # Tempo medio de internacao ao obito
            '''
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/tempo-medio-internacao?data={}&idMunicipio='.format(date.date()))
            df3 = pd.DataFrame([pd.read_json(response.text, typ='series')])
            if not 'tempoMedio' in df3:
                df1['Tempo Medio Internacao ao Obito']=0
            else:
                df1['Tempo Medio Internacao ao Obito'] = round(df3['tempoMedio'],3)
            '''

            # Porcentagem de comorbidade
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/proporcao-comorbidades?data={}&idMunicipio='.format(date.date()))
            df4 = pd.DataFrame([pd.read_json(response.text, typ='series')])
            df1['Comorbidade (%)'] = df4['proporcaoComorbidade']

            # Idade média e mediana óbito
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/media-idade?data={}&idMunicipio='.format(date.date()))
            df5 = pd.DataFrame([pd.read_json(response.text, typ='series')])
            df1['Idade media obito'] = df5['idadeMedia']
            df1['Idade mediana obito'] = df5['idadeMediana']

            # Óbitos suspeitos
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/obitos-suspeitos?data={}&idMunicipio='.format(date.date()))
            df6 = pd.DataFrame([pd.read_json(response.text, typ='series')])
            df1['Obitos suspeitos'] = df6['quantidadeObitos']

            # Tempo medio internacao, obito e resultado exame
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/tempo-medio-espera?data={}&idMunicipio='.format(date.date()))
            df7 = pd.read_json(response.text)
            df1['Dias entre inicio sintomas ate internacao'] = df7['tempoMedio'][0]
            df1['Dias entre inicio sintomas ate resultado do exame'] = df7['tempoMedio'][1]
            df1['Dias entre inicio sintomas ate obito'] = df7['tempoMedio'][2]

            # Local do óbito
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/local-obito?data={}&idMunicipio='.format(date.date()))
            df8 = pd.read_json(response.text)
            df8

            if 'PRIVADA' in df8['localObito'].values:
                df1['Obitos hospitais privados'] = df8.loc[df8['localObito'] == 'PRIVADA', 'quantidadeObitos'].iloc[0]
            else:
                df1['Obitos hospitais privados'] = 0

            if 'DOMICILIAR' in df8['localObito'].values:
                df1['Obitos na residencia'] = df8.loc[df8['localObito'] == 'DOMICILIAR', 'quantidadeObitos'].iloc[0]
            else:
                df1['Obitos na residencia'] = 0

            if 'PÚBLICA' in df8['localObito'].values:
                df1['Obitos hospitais publicos'] = df8.loc[df8['localObito'] == 'PÚBLICA', 'quantidadeObitos'].iloc[0]
            else:
                df1['Obitos hospitais publicos'] = 0

            if 'SEM INFORMAÇÃO' in df8['localObito'].values:
                df1['Obitos sem informacao local'] = df8.loc[df8['localObito'] == 'SEM INFORMAÇÃO', 'quantidadeObitos'].iloc[0]
            else:
                df1['Obitos sem informacao local'] = 0

            #Colocar data
            df1.insert(loc=0, column='Data', value=date.date())
            df1['Casos confirmados'] = df1['Casos confirmados'].astype(int)
            df1['Total obitos'] = df1['Total obitos'].astype(int)

            filename = r'ObitosResumo_'+data_final.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
            if writeHeader is True:
                df1.to_csv(filename, mode='a', encoding='utf-16', index=None)
                writeHeader = False
            else:
                df1.to_csv(filename, mode='a', encoding='utf-16', header=False, index=None)

            date += datetime.timedelta(days=1)
        enviar_github('covid19-Ceara', filename, 'ObitosResumo_CE/'+filename, 'Atualizado em: ' + data_final.strftime('%d/%m/%Y %H:%M:%S'))


    def obitos_comorbidades(data_final):
        response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/limite-data?data=&idMunicipio=')
        dfinicial = pd.DataFrame([pd.read_json(response.text, typ='series')])
        date = dfinicial['dataMin'][0]
        date = datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        writeHeader = True

        while date<=data_final:
            #print(date.date())
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/comorbidade?data={}&idMunicipio='.format(date.date()))
            df1 = pd.read_json(response.text)
            df2 = df1[df1['situacaoComorbidade']=='Sim']
            df2.insert(loc=0, column='Data', value=date.date())
            df2 = df2.sort_values(by=['quantidadeObitos'], ascending=False)

            filename = r'Comorbidades_'+data_final.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
            if writeHeader is True:
                df2.to_csv(filename, mode='a', encoding='utf-16', index=None)
                writeHeader = False
            else:
                df2.to_csv(filename, mode='a', encoding='utf-16', header=False, index=None)
            date += datetime.timedelta(days=1)
        enviar_github('covid19-Ceara', filename, 'Comorbidades_CE/'+filename, 'Atualizado em: ' + data_final.strftime('%d/%m/%Y %H:%M:%S'))

    def faixa_etaria_obitos(data_final):
        response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/limite-data?data=&idMunicipio=')
        dfinicial = pd.DataFrame([pd.read_json(response.text, typ='series')])
        date = dfinicial['dataMin'][0]
        date = datetime.datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        writeHeader = True

        while date<=data_final:
            #print(date.date())
            response = requests.get('https://indicadores.integrasus.saude.ce.gov.br/api/obitos-covid/faixa-etaria?data={}&idMunicipio='.format(date.date()))
            df1 = pd.read_json(response.text)

            df1.insert(loc=0, column='Data', value=date.date())

            filename = r'FaixaEtariaObitos_'+data_final.strftime('%Y-%m-%d_%H_%M_%S')+'.csv'
            if writeHeader is True:
                df1.to_csv(filename, mode='a', encoding='utf-16', index=None)
                writeHeader = False
            else:
                df1.to_csv(filename, mode='a', encoding='utf-16', header=False, index=None)
            date += datetime.timedelta(days=1)
        enviar_github('covid19-Ceara', filename, 'FaixaEtariaObitos_CE/'+filename, 'Atualizado em: ' + data_final.strftime('%d/%m/%Y %H:%M:%S'))


    # --- EXECUTAR ---
    municipios(data_inicial, data_final)
    resultados_exames(data_inicial, data_final)
    faixa_etaria(data_inicial, data_final)
    hospital_vinci()
    #obitos_resumo(data_final)
    #obitos_comorbidades(data_final)
    #faixa_etaria_obitos(data_final)

    toc = time.time()
    get_time=round(toc-tic,3)
    print('Finished in ' + str(get_time) + ' seconds')


schedule.every(3).hours.do(job)

# CUIDADO SE FOR COPIAR PARA O HEROKU AJUSTAR FUSO
while True:
    data_inicial = datetime.datetime(2020, 1, 1, 0, 0, 0, 0)
    data_final = datetime.datetime.now() - datetime.timedelta(hours=3)

    schedule.run_pending()
    time.sleep(1)
