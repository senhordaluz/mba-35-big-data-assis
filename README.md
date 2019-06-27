# Interface Cadastro Positivo com Apache Spark

Projeto desenvolvido como trabalho final da disciplina de **Big Data Science** das turmas integradas *UNIF.SOFT.35* *BIB8* e *INOVAÇÃO2* dos cursos de **MBA da Escola Politécnica da UFRJ** lecionada pelo professor [Alexandre A. B. Lima](mailto:assis@cos.ufrj.br).

[Link para a apresentação](https://drive.google.com/open?id=1cjppE1vkkUlBSqmM4s4hAYLHzPbB8nmg).

## Idealizadores do projeto

- [Edilson Pereira de Andrade](mailto:adr.edilson@gmail.com)
- [Jorge Alberto](mailto:joabergon@gmail.com)
- [Pedro da Luz](mailto:pedro256@gmail.com)
- [Thiago Silva](mailto:tagalho1609@gmail.com)

## Como executar

Para executar o script, basta executar o seguinte comando:

```ssh
spark-submit trabalho-final.py
```

Ao final do script, surgirão duas pastas `trabalho-final.csv` e `trabalho-final-com-virgula.csv`, ambas contendo um arquivo de uma única partição *(no formato interno do hadoop)*. Sendo o primeiro um arquivo `csv` separado por ponto e vírgula *(utilizado durante a apresentação pelo Power Bi)* enquanto o segundo sendo um arquivo `csv` padrão separado por vírgulas para melhor visualização em plataformas Linux.

## Dados

Temos dois exemplos de dados utilizados como arquivo fonte.

- `PositivoV02.csv`
- `PositivoV03.csv`

Ambos podem ser tratados pelo script `trabalho-final.py`
