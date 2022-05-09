from modules.mongoDB import Conector_mongoDB
from modules.postgres import Conector_postgres
from modules.graficos import Grafico
import pandas as pd

if __name__=="__main__":
    try:
        print("Conectando com o MongoDB...")
        bancoMongoDB=Conector_mongoDB()
        
        print("Conectando com o Postgres")
        bancoPostgres=Conector_postgres()
        
        while True:
            escolha=input("O que você deseja fazer?\n1-Extração dos dados CVS em um DF e inserção no mongoDB\n2-Tratamento dos dados e inserção no postgre\n3-Insights\n0-Sair\n")       
        
            if escolha=="1":    
                print("Importando dados do CSV em um DF")
                ocorrencias=pd.read_csv("https://storage.googleapis.com/dadosbrutossca/ocorrencias.csv",sep=";")
                        
                print("Inserindo dados no mongoDB...")
                bancoMongoDB.insert(ocorrencias.to_dict("records"))
                print("Dados inseridos com sucesso!\n------------------------------------------")    
            
            elif escolha=="2":
                df=pd.DataFrame(bancoMongoDB.find())#Lendo os dados do mongoDB em um DF
            
                print("Tratando os dados...")
                df.drop(columns=["codigo_ocorrencia1","codigo_ocorrencia2","codigo_ocorrencia3","codigo_ocorrencia4"],inplace=True) #Retirando colunas repetidas
                df.drop_duplicates(inplace=True)#Retirando valores repetidos
                
                #Tratando a coluna "ocorrencia_latitude"
                df["ocorrencia_latitude"].replace(r"[a-z]", "", regex=True,inplace=True)#Retirar todas as letras minusculas dos valores da coluna "ocorrencia_latitude"
                df["ocorrencia_latitude"].replace(r"[A-Z]", "", regex=True,inplace=True)#Retirar todas as letras maiusculas dos valores da coluna "ocorrencia_latitude"
                
                #Tratando a coluna "ocorrencia_longitude"
                df["ocorrencia_longitude"].replace(r"[a-z]", "", regex=True,inplace=True)#Retirar todas as letras minusculas dos valores da coluna "ocorrencia_longitude"
                df["ocorrencia_longitude"].replace(r"[A-Z]", "", regex=True,inplace=True)#Retirar todas as letras maiusculas dos valores da coluna "ocorrencia_longitude"
                
                #Tratando valores de todas as colunas
                df.replace(r"[\\]", "", regex=True, inplace=True)#Retirar "\" dos valores de qualquer coluna
                df.replace(r"[*]", None, regex=True, inplace=True)#Deixar os valores NaN onde tem *
                df.replace(r"[#]", None, regex=True, inplace=True)#Deixar os valores NaN onde tem #
                df.replace(r"[°]", "", regex=True, inplace=True)#Retirar "°" dos valores de qualquer coluna
                df.replace(r"[']", "", regex=True, inplace=True)#Retirar " ' " dos valores de qualquer coluna
                df.fillna("nulo", inplace=True)#Substituindo os valores NaN por "nulo"
                
                #Alterando o formato da data para o formato ano/mês/dia, YYYY-mm-dd 
                df["ocorrencia_dia"]=pd.to_datetime(df["ocorrencia_dia"],format="%d/%m/%Y")
                df["divulgacao_dia_publicacao"]=pd.to_datetime(df["divulgacao_dia_publicacao"],format="%d/%m/%Y",errors="ignore")
                
                print("Criando tabela no postgre")
                criar_tabela='''CREATE TABLE IF NOT EXISTS ocorrencias (
                    codigo_ocorrencia int,
                    ocorrencia_classificacao varchar(30),
                    ocorrencia_latitude varchar(20),
                    ocorrencia_longitude varchar(20),
                    ocorrencia_cidade varchar(50),
                    ocorrencia_uf varchar(5),
                    ocorrencia_pais varchar(30),
                    ocorrencia_aerodromo varchar(20),
                    ocorrencia_dia date,
                    ocorrencia_hora varchar(30),
                    investigacao_aeronave_liberada varchar(10),
                    investigacao_status varchar(30),
                    divulgacao_relatorio_numero varchar(50),
                    divulgacao_relatorio_publicado varchar(5),
                    divulgacao_dia_publicacao varchar(20),
                    total_recomendacoes int,
                    total_aeronaves_envolvidas int,
                    ocorrencia_saida_pista varchar(5),    
                    CONSTRAINT ocorrencias_pk PRIMARY KEY (codigo_ocorrencia)                   
                    );'''
                bancoPostgres.executar(criar_tabela)
                
                print("Inserindo dados tratados no Postgre...")
                for i,x in df.iterrows():
                    bancoPostgres.executar(f'''INSERT INTO ocorrencias VALUES ({int(df['codigo_ocorrencia'][i])},
                                        '{df['ocorrencia_classificacao'][i]}','{df['ocorrencia_latitude'][i]}',
                                        '{df['ocorrencia_longitude'][i]}', '{df['ocorrencia_cidade'][i]}',
                                        '{df['ocorrencia_uf'][i]}','{df['ocorrencia_pais'][i]}',
                                        '{df['ocorrencia_aerodromo'][i]}', date '{df['ocorrencia_dia'][i]}',
                                        '{df['ocorrencia_hora'][i]}','{df['investigacao_aeronave_liberada'][i]}',
                                        '{df['investigacao_status'][i]}', '{df['divulgacao_relatorio_numero'][i]}',
                                        '{df['divulgacao_relatorio_publicado'][i]}', 
                                        '{df['divulgacao_dia_publicacao'][i]}', {int(df['total_recomendacoes'][i])},
                                        {int(df['total_aeronaves_envolvidas'][i])}, 
                                        '{df['ocorrencia_saida_pista'][i]}')''')
                print("Dados inseridos com sucesso!")
                                
            elif escolha=="3":
                while True:
                    print("-------------------------------------------------------------")  
                    escolha2=input("Escolha um Insight:\n1-Os 5 estados com maior qtde de aeronaves envolvidas em ocorrencias\n2-Qtde de ocorrencias por mês no 1º semestre de 2012\n3-Investigação finalizada x Investigação ativa \n0-Sair\n")
                    
                    if escolha2=="1":
                        select1=bancoPostgres.selecionar(f"SELECT SUM(total_aeronaves_envolvidas)as qtde_aeronaves, ocorrencia_uf  FROM ocorrencias GROUP BY ocorrencia_uf ORDER BY qtde_aeronaves DESC LIMIT 5")
                        
                        estados=[select1[0][1],select1[1][1],select1[2][1],select1[3][1],select1[4][1]]
                        qtde_aeronaves=[select1[0][0],select1[1][0],select1[2][0],select1[3][0],select1[4][0]]
                        
                        Grafico.plotBarra(estados,qtde_aeronaves,"blue","Os 5 estados com maior qtde de aeronaves envolvidas em ocorrencias","Estados","Qtde_aeronaves")
                    
                    elif escolha2=="2":
                        janeiro=bancoPostgres.selecionar(f"SELECT COUNT(codigo_ocorrencia) as qtde_ocorrencias FROM ocorrencias WHERE ocorrencia_dia BETWEEN '2012-01-01' and '2012-01-31'")
                        fevereiro=bancoPostgres.selecionar(f"SELECT COUNT(codigo_ocorrencia)as qtde_ocorrencias FROM ocorrencias WHERE ocorrencia_dia BETWEEN '2012-02-01' and '2012-02-29'")
                        marco=bancoPostgres.selecionar(f"SELECT COUNT(codigo_ocorrencia)as qtde_ocorrencias FROM ocorrencias WHERE ocorrencia_dia BETWEEN '2012-03-01' and '2012-03-31'")
                        abril=bancoPostgres.selecionar(f"SELECT COUNT(codigo_ocorrencia)as qtde_ocorrencias FROM ocorrencias WHERE ocorrencia_dia BETWEEN '2012-04-01' and '2012-04-30'")
                        maio=bancoPostgres.selecionar(f"SELECT COUNT(codigo_ocorrencia)as qtde_ocorrencias FROM ocorrencias WHERE ocorrencia_dia BETWEEN '2012-05-01' and '2012-05-30'")
                        junho=bancoPostgres.selecionar(f"SELECT COUNT(codigo_ocorrencia)as qtde_ocorrencias FROM ocorrencias WHERE ocorrencia_dia BETWEEN '2012-06-01' and '2012-06-30'")
                        qtde_mes=[janeiro[0][0],fevereiro[0][0],marco[0][0],abril[0][0],maio[0][0],junho[0][0]]
                        mes=["Janeiro","Fevereiro","Março","Abril","Maio","Junho"]
                        
                        Grafico.plotBarra(mes,qtde_mes,"blue","Qtde de ocorrencias por mês no 1º semestre de 2012","Meses","Qtde de ocorrencias")
                    
                    elif escolha2=="3":
                        finalizada=bancoPostgres.selecionar(f"SELECT COUNT(investigacao_status) FROM ocorrencias WHERE investigacao_status='FINALIZADA'")
                        ativa=bancoPostgres.selecionar(f"SELECT COUNT(investigacao_status) FROM ocorrencias WHERE investigacao_status='ATIVA'") 
                        
                        qtde_investigacoes=[finalizada[0][0],ativa[0][0]]
                        status=["Finalizada","Ativa"] 
                        
                        Grafico.plotBarra(status,qtde_investigacoes,"blue","Investigação finalizada x Investigação ativa","Status da investigação","Qtde de investigações")
                        
                    elif escolha2=="0":
                        break
                    
                    else:
                        print("Faça uma escolha válida!")    
            
            elif escolha=="0":
                break
            
            else:
                print("Faça uma escolha válida!")
                
    except Exception as e:
        print(str(e))