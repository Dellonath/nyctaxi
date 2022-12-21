# Projeto New York Taxi

Este README apresenta apenas uma breve descrição do problema bem como a solução implementada.

## Objetivo
Construir um pipeline de engenharia e responder a algumas perguntas realizadas pelo time de negócio.

## Dados
Foi fornecido quatro conjunto de dados em formato `.json` relacionando viagens de táxi na cidade de Nova York nos anos de 2009, 2010, 2011 e 2012, cada ano em um arquivo. Este conjunto é composto de uma série de informações como o preço da viagem, a distância do trajeto, as coordenadas de partida e chegada, o método de pagamento, etc.

## Tecnologias
- Para armazenar os dados, tanto bruto como processado, a ferramenta `S3` foi escolhida;
- Para a construção do pipeline de engenharia de dados foi utilizado o `AWS Glue`;
- A análise foi realizada em um ambiente de jupyter notebook, logo utilizou-se de uma instância no `Sagemaker`;
- A linguagem de programação utilizada foi a `Python`.

## Estrutura do repositório

    ├── README.md                      <- Breve descrição do projeto e ferramentas
    ├── engenharia                     <- Concentra tudo que for relacionado ao pipeline de engenharia de dados
    │   ├── imagens                    <- Concentra prints do pipeline de engenharia de dados
    │   └── scripts                    
    │       ├── raw-to-trusted         <- Script responsável por ler o dado bruto, processar e jogar para a próxima camada (trusted)
    │       └── trusted-to-refined     <- Script responsável por ler o dado processado, aplicar regra de negócio e jogar para a próxima camada (refined)
    └── analise                        <- Concentra tudo que for relacionado a análise de dados
        ├── imagens                    <- Concentra prints das análises
        └── nyctaxy-eda.ipynb          <- Jupyter Notebook com as análises realizadas          
    
## S3
Camadas:
- **raw**: armazena os dados brutos, da forma como vieram em `.json`. Apenas houve a renomeação dos nomes dos arquivos. Mas a estrutura interna não foi alterada;
- **trusted**: armazena os dados processados. Nesta etapa é realizada a renomeação de colunas, ajuste de datas e horas e conversão para tipo `.parquet`; 
- **refined**: camada onde é aplicada regras de negócios. Neste caso, a regra de negócio foi a seleção apenas das colunas necessárias para responder o problema de negócio e o Union dos anos. 

Bucket deve ter o nome `dadosfera-dev`, uma pasta `datalake` também deve ser criada, em seguida deve ser separado as camadas.

## AWS Glue






