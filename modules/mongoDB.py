from pymongo import MongoClient

class Conector_mongoDB():
    def __init__(self, database, collection, str_conexao):
        '''Construtor da classe Conector_mongoDB utilizando o modulo pymongo

        Args:
            database (string): Nome do banco de dados 
            collection (string): Nome da coleção
            str_conexao (string): String de conexão com o mongoDB
        '''
        try:
            self.cliente = MongoClient(str_conexao)
            self.database =  self.cliente[database]
            self.collection =self.database[collection]
        except Exception as e:
            print(str(e))
        
    def set_database(self,database):
        '''Escolhe o database
        
        Args:
            database: Nome do database
        '''
        try:
            self.database = database
        except Exception as e:
            print(str(e))
        
    def set_collection(self, collection):
        '''Escolhe a coleção
        
        Args:
            collection: Nome da coleção
        '''
        try:
            self.collection = collection  
        except Exception as e:
            print(str(e))
                
    def get_database(self):
        '''Retorna o database
        
        Returns: 
            self.database: Retorna o nome do database
        '''
        try:
            return self.database
        except Exception as e:
            print(str(e))
            
    def get_collection(self):
        '''Retorna a coleção
        
        Returns:
            self.collection: Retorna o nome da coleção
        '''
        try:    
            return self.collection
        except Exception as e:
            print(str(e))
        

    # Métodos da classe:
    def insert(self,dados):
        '''Insere dados em um banco mongoDB
        
        Args:
            dados: dados que vão ser inseridos no banco de dados
        '''
        try:
            self.collection.insert_many(dados)
        except Exception as e:
            print(str(e))
        
    def find(self):
        '''Busca dados em um banco mongoDB
        
        Returns:
            lista_itens: Retorna os dados pesquisados em uma lista
        '''
        try:
            lista_itens = []
            itens_db = self.collection.find()
            for i in itens_db:
                lista_itens.append(i)
            return lista_itens
        except Exception as e:
            print(str(e))
                
    def delete_one(self):
        '''Deleta um dado de um banco mongoDB'''
        try:
            coluna = input("Você deseja excluir por qual dado? ")
            
            valor = input("Qual o valor desse dado do item que você deseja excluir? ")
            
            filter = {coluna: valor}
                
            self.collection.delete_one(filter)
        except Exception as e:
            print(str(e))
                
    def delete_many(self):
        '''Deleta dados de um banco mongoDB'''
        try:
            coluna = input("Você deseja excluir por qual dado? ")
            
            valor = input("Qual o valor desse dado dos itens que você deseja excluir? ")
            
            filter = {coluna: valor}
                
            self.collection.delete_many(filter)
        except Exception as e:
            print(str(e))
                
    
    def update_one(self):
        '''Atualiza um dado de um banco mongoDB'''
        try:    
            coluna_escolhida = input("Digite a coluna que você deseja realizar uma alteração: ")
            old_value = input("Digite o valor antigo desse item nessa coluna: ")
            new_value= input("Digite o novo valor para esse item: ")
            
            
            filter = {coluna_escolhida: old_value}
            newvalues = {"$set": {coluna_escolhida: new_value}}
            
            self.collection.update_one(filter,newvalues)
        except Exception as e:
            print(str(e))
                    