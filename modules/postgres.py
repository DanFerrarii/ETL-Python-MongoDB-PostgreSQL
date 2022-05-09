import psycopg2

class Conector_postgres:

    def __init__ (self, host, db, user, password):
        '''Construtor da classe Conector_postgres utilizando o modulo psycopg2

        Args:
            host (string): endereco do host
            db (string): nome do banco de dados
            user (string): usuario para conexao ao banco de dados
            senha (string): senha para acesso ao banco de dados
        '''
        try:
            self.host = host
            self.db = db
            self.user = user
            self.password = password
        except Exception as e:
            print(str(e))
            
    def conectar (self):
        '''Conecta no BD
        
        Returns:
            conn: Conector SQL
            cursor: Cursor para leitura do banco de dados
        '''
        try:
            conn = psycopg2.connect( host=self.host, database=self.db, user=self.user, password=self.password)
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            print(str(e))
            
    def desconectar(self, conn, cursor):
        '''Desconecta do BD
        
        Args:
            conn: Conector SQL
            cursor: Cursor para leitura do banco de dados
        '''
        try:
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(str(e))
    
    def executar(self, query):
        '''Inseri, altera ou deleta informações no BD
        
        Args:
            query (string): Query que executa uma ação no bando de dados
        '''
        try:
            conn, cursor = self.conectar()
            cursor.execute(query)
            self.desconectar(conn, cursor)
        except Exception as e:
            print(str(e))
            
    def selecionar(self, query):
        '''Seleciona informações do BD
        
        Args:
            query (string): Query para pesquisar dados no banco de dados
            
        Returns:
            lista_dados: Retorna os dados encontrados em um formato de lista
        '''
        try:
            conn, cursor = self.conectar()
            cursor.execute(query)
            dados = cursor.fetchall()
            self.desconectar(conn, cursor)
            lista_dados = []
            for dado in dados:
                lista_dados.append(dado)
            return lista_dados
        except Exception as e:
            print(str(e))
    