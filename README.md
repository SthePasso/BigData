# bdg-pis

Este repositório se trata da matéria Prática em Banco de Dados. 

## Trabalho 1
Para o pi1p1 deve ser colocado a pasta hadoop na "Pasta Pessoal" e lá terá um script .profile que deve ser alterado com mais essas duas linhas:

export PATH="$HOME/hadoop/bin:$PATH"

export JAVA_HOME=/usr/lib/jvm/default-java

Após ser alterado e salvo o computador deve ser reiniciado pois assim o hadoop estará visivel para todos os programas do pc

Para rodar o experimento entre no diretório base  bdg-pis/pi1p1, rode o Maven para criar seu package:

mvn clean package

hadoop jar target/pi1p1-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.SthePasso.WordCount -input Shakespeare.txt -output wc -reducers 5

Esses comandos estão funcionando no computador pessoal. 

## Trabalho 2
