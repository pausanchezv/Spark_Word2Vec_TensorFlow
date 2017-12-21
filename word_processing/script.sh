echo "Compilant el fitxer i creant executables.."
scalac CleanText.scala
echo "Netejant els textos.."
scala CleanText
echo "Eliminant els fitxers de compilació '.class'"
rm *.class
echo "Ara el fitxer quijote net és a la ruta: '../files/clean_quijote.txt'"


