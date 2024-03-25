import requests
import pandas as pd
import math
from io import BytesIO
import requests
import json
import re
import hashlib
import subprocess
from datetime import datetime


COL_UNIDADE_GESTORA = 2

COL_NR_CONTRATO = 0
COL_OBJETO = 3
COL_DATA_PUB = 7
COL_NR_EDITAL = 8
COL_INICIO = 10
COL_TERMINO = 11
COL_SITUACAO = 12
COL_VALOR_TOTAL = 17

COL_ITEM_FORNECIDO = 13
COL_UNIDADE_MEDIDA = 14
COL_VALOR_UNITARIO = 15
COL_QUANTIDADE = 16
COL_VALOR_TOTAL_ITEM = 17

COL_CONTRATADO = 19
COL_CPF_CNPJ = 22

COL_SOCIOS = 23

contratosByUnidade = {}
contratados = {}
sociosDic = {}


def get_codigo_ug(s):
    match = re.search(r'\d+', s)
    if match:
        return match.group()
    else:
        return None


def download_and_convert_to_dataframe(url):
    response = requests.get(url)
    if response.status_code == 200:
        file_content = BytesIO(response.content)

        df = pd.read_excel(file_content, engine='odf')

        df.fillna('')

        return df
    else:
        print("Failed to download the file.")
        return None


def extract_cpf_or_cnpj_and_name(text):
    # Regular expression to match CPF or CNPJ and name
    regex = r'((\d{3}.\d{3}.\d{3}-\d{2})\s-\s(.*))|((\d{2}.\d{3}.\d{3}/\d{4}-\d{2})\s-\s(.*))'

    # Search for CPF or CNPJ and name
    match = re.search(regex, text)
    if match:
        cpf_cnpj = match.group(2) or match.group(5)
        name = match.group(3) or match.group(6)
        return cpf_cnpj, name
    else:
        return 'None', None


def parse(df):
    ug = ''

    pularProxima = False
    
    for index, row in df.iterrows():
        # if index == 0:
        #    continue

        if str(row.iloc[COL_NR_CONTRATO]) == 'nan':
            ug = row.iloc[COL_UNIDADE_GESTORA]

            if ug != '':
                contratosByUnidade[ug] = {}

            pularProxima = True
            continue

        if pularProxima:
            pularProxima = False
            continue

        if ug == '':
            continue

        contratos = contratosByUnidade[ug]

        nrContrato = row.iloc[COL_NR_CONTRATO]
        nrContrato = nrContrato.replace('/', '.')
        if nrContrato not in contratos:
            contratos[nrContrato] = {
                "contratado": row.iloc[COL_CPF_CNPJ],
                "objeto": row.iloc[COL_OBJETO],
                "data_publicacao": row.iloc[COL_DATA_PUB],
                "nr_edital": row.iloc[COL_NR_EDITAL],
                "inicio": row.iloc[COL_INICIO],
                "termino": row.iloc[COL_TERMINO],
                "situacao": row.iloc[COL_SITUACAO],
                "valor_total": row.iloc[COL_VALOR_TOTAL],
                "ug": get_codigo_ug(ug),
            }

        if "itens" not in contratos[nrContrato]:
            contratos[nrContrato]['itens'] = []

        contratos[nrContrato]['itens'].append(
            {
                'item_fornecido':  row.iloc[COL_ITEM_FORNECIDO],
                'unidade_de_medida': row.iloc[COL_UNIDADE_MEDIDA],
                'valor_unitario': row.iloc[COL_VALOR_UNITARIO],
                'quantidade': row.iloc[COL_QUANTIDADE],
                'valor_total': row.iloc[COL_VALOR_TOTAL_ITEM],
            }
        )

        cpf_cnpj = row.iloc[COL_CPF_CNPJ]
        if cpf_cnpj not in contratados:
            contratados[cpf_cnpj] = {
                'nome': row.iloc[COL_CONTRATADO],
                'tem_contrato_ativo': row.iloc[COL_SITUACAO].lower() == 'ativo',
            }

            #print(row.iloc[COL_SITUACAO])
            #print(row.iloc[COL_SITUACAO] == 'ativo')
        else:
            if row.iloc[COL_SITUACAO].lower() == 'ativo':
                contratados[cpf_cnpj]['tem_contrato_ativo'] = True

        if type(row.iloc[COL_SOCIOS]) == str:
            socios = row.iloc[COL_SOCIOS]
            sociosList = socios.split(";")
        else:
            print(row.iloc[COL_SOCIOS])

        for socio in sociosList:
            s = extract_cpf_or_cnpj_and_name(socio)

            if s[0] not in sociosDic:
                sociosDic[s[0]] = {
                    'nome': s[1]
                }

        #if index > 50:
        #    break


def getTotal(x):
    try:
        return float(non_decimal.sub('', contr['valor_total']).replace(',', '.'))
    except:
        return 0.0


def insert_to_firestore():
    PROJECT_ID = "projeto-programacao-movel"
    API_BASE = "https://firestore.googleapis.com/v1/projects/{}/databases/(default)/".format(
        PROJECT_ID)

    result = subprocess.run(
        ['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=subprocess.PIPE)
    TOKEN = result.stdout.decode('utf-8').strip()

    print(TOKEN)

    for key, value in contratosByUnidade.items():

        for nrCont, contr in value.items():
            API_URL = "{}documents/{}/{}/".format(API_BASE,
                                                  'contratos', nrCont)

            #print(contr['data_publicacao'])
            #print(math.isnan(contr['data_publicacao']))

            non_decimal = re.compile(r'[^\d,]+')

            itens = []

            for idx, item in enumerate(contr['itens']):
                URL = API_URL + "itens/{}".format(idx)
                print(URL)
                itens.append({
                    'mapValue': {
                        'fields': {
                            'item_fornecido': {
                                "stringValue": item['item_fornecido']
                            },
                            'valor_unitario': {
                                "stringValue": item['valor_unitario']
                            },
                            'quantidade': {
                                "stringValue": item['quantidade']
                            },
                            'unidade_de_medida': {
                                "stringValue": item['unidade_de_medida']
                            },
                            'valor_total': {
                                "stringValue": item['valor_total']
                            }
                        }
                    }
                })

            data = {
                'fields': {
                    'nr_contrato': {
                        "stringValue": nrCont.replace('.', '/')
                    },
                    'contratado': {
                        "stringValue": contr['contratado']
                    },
                    'objeto': {
                        "stringValue": contr['objeto']
                    },
                    'data_publicacao': {
                        "stringValue": "'{}'".format(contr['data_publicacao'])
                    },
                    'nr_edital': {
                        "stringValue": contr['nr_edital']
                    },
                    'inicio': {
                        "timestampValue": "{}".format(datetime.strptime(contr['inicio'], "%d/%m/%Y").isoformat() + 'Z')
                    },
                    'termino': {
                        "timestampValue": "{}".format(datetime.strptime(contr['termino'], "%d/%m/%Y").isoformat() + 'Z')
                    },
                    'situacao': {
                        "stringValue": contr['situacao'].lower()
                    },
                    'valor_total': {
                        "doubleValue": getTotal(contr['valor_total'])
                    },
                    'ug': {
                        "stringValue": contr['ug']
                    },
                    'itens': {
                        'arrayValue': {
                            "values": itens
                        }
                    }
                }
            }

            r = requests.patch(API_URL, headers={
                "Content-Type": "application/json", 'Authorization': 'Bearer {}'.format(TOKEN)}, json=data)

            if r.status_code != 200:
                print(r)
                print(r.content)

    for key, value in contratados.items():
        API_URL = "{}documents/{}/{}/".format(API_BASE,
                                              'contratados', re.sub("[^0-9]", "", key))

        data = {
            'fields': {
                'cpf_cnpj': {
                    "stringValue": key
                },
                'nome': {
                    "stringValue": value['nome']
                },
                'tem_contrato_ativo': {
                    "booleanValue": value['tem_contrato_ativo']
                }
            }
        }

        r = requests.patch(API_URL, headers={
                           "Content-Type": "application/json", 'Authorization': 'Bearer {}'.format(TOKEN)}, json=data)


    for key, value in sociosDic.items():
        API_URL = "{}documents/{}/{}/".format(API_BASE,
                                              'socios', re.sub("[^0-9]", "", key))

        data = {
            'fields': {
                'cpf_cnpj': {
                    "stringValue": key
                },
                'nome': {
                    "stringValue": value['nome']
                }
            }
        }

        r = requests.patch(API_URL, headers={
                           "Content-Type": "application/json", 'Authorization': 'Bearer {}'.format(TOKEN)}, json=data)



def extract_year_and_month(string):
    year = string[:4]
    month = string[4:]

    month_names = {
        '01': 'Janeiro',
        '02': 'Fevereiro',
        '03': 'Março',
        '04': 'Abril',
        '05': 'Maio',
        '06': 'Junho',
        '07': 'Julho',
        '08': 'Agosto',
        '09': 'Setembro',
        '10': 'Outubro',
        '11': 'Novembro',
        '12': 'Dezembro'
    }

    return year, month_names.get(month)



#
#
#

import sys

def main():
    if len(sys.argv) > 1:
        first_arg = sys.argv[1]

        result = extract_year_and_month(first_arg)
        url = "http://www.transparencia.mpf.mp.br/conteudo/licitacoes-contratos-e-convenios/contratos/{}/contratos_{}_{}.ods".format(
            result[0], result[0], result[1])
        df = download_and_convert_to_dataframe(url)

        parse(df)

        insert_to_firestore()
    else:
        # Notify the user that no argument was provided
        print("No argument provided.")

if __name__ == "__main__":
    main()
