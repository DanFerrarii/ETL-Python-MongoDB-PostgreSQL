import matplotlib.pyplot as plt

class Grafico:
    def __init__(self,eixox,eixoy,cor,titulo,nome,nome_eixoX,nome_eixoY,valores_x,valores_y):
        try:
            self.eixox=eixox
            self.eixoy=eixoy
            self.cor=cor
            self.titulo=titulo
            self.nome=nome
            self.nome_eixoX=nome_eixoX
            self.nome_eixoY=nome_eixoY
            self.valores_x=valores_x 
            self.valores_y=valores_y
        except Exception as e:
            print(str(e)) 

    def plotBarra(eixox,eixoy,cor,titulo,nome_eixoX,nome_eixoY,valores_x=None,valores_y=None):
        '''Cria um gráfico de barras e exibe o mesmo 
        
        Args:
            eixox (lista): Dados do eixo X
            eixoy (lista): Dados do eixo Y
            cor: Cor da barra do gráfico, Ex: "blue", "red", "yellow"
            titulo: Titulo do gráfico
            nome_eixoX: Nome do eixo X
            nome_eixoY: Nome do eixo Y
            valores_x: Definir os valores do eixo X, caso seja necessário
            valores_y: Definir os valores do eixo Y, caso seja necessário
        '''
        try:    
            plt.bar(eixox,eixoy,color=cor)
            plt.title(titulo)
            plt.xlabel(nome_eixoX)
            plt.ylabel(nome_eixoY)
            plt.xticks(valores_x)
            plt.yticks(valores_y)
            return plt.show()
        except Exception as e:
            print(str(e))