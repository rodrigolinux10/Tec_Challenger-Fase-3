# Tec_Challenger-Fase-3
Tec Challenger Fase 3  - ETL base Pnad-Covid-19




<img width="786" height="462" alt="image" src="https://github.com/user-attachments/assets/ab83f42e-4697-438c-aa05-e818ac10b2f6" />





Essa etapa foi voltada ao universo do Big Data aplicado a projetos de ETL, com foco em como estruturar pipelines capazes de transformar dados brutos em informação qualificada para análise.

No projeto, partimos dos arquivos CSV da PNAD COVID-19 (IBGE), que foram armazenados no Amazon S3. Em seguida, utilizamos o AWS Glue para realizar processos de extração, limpeza e transformação dos dados, sempre monitorados pelo CloudWatch na região us-east-1.

A arquitetura foi organizada em camadas:

Bronze: dados brutos, sem tratamento.

Silver: dados limpos e padronizados.

Gold: dados agregados e refinados, prontos para análise.

Por fim, os dados da camada Gold foram integrados em um dashboard interativo, permitindo visualizar indicadores relevantes da pandemia e gerar insights estratégicos.
