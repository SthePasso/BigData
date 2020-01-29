# bdg-pis
Este repositório se trata da matéria Prática em Banco de Dados. Para rodar o experimento entre no diretório base  bdg-pis/pi1p1, rode o Maven para criar seu package:

mvn clean package

hadoop jar target/pi1p1-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.SthePasso.WordCount -input Shakespeare.txt -output wc -reducers 5

Esses comandos estão funcionando no computador pessoal. 
