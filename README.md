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

O arquivo de entrada de onde serão extraídos os dados de entrada é o “Amazon product co-purchasing network metadata” que faz parte do Stanford Network Analysis Project (SNAP). Os dados foram coletados em 2006 do site Amazon e informações de produto e comentários de clientes sobre 548.552 produtos diferentes (livros, CDs de música, DVDs e fitas de vídeo VHS).

Esse arquivo deve ser baixado e colocado no mesmo diretório de "PI2_Sthefanie_Passo.py" 

Assim temos diferentes consultas de 'a' até 'g' que podem ser realizadas, e assim pode-se executar todas de uma vez ou uma por vez comentando as outras
