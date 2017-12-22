import scala.io.Source
import java.io._

/**
  * Pràctica 3 de Bases de Dades Avançades
  * 
  * Pau Sanchez i Guillem Rabionet
  *
  * Objecte de funcions que proporciona utilitats diverses per la pràctica
  */
object CleanText {

    // es tenen en compte les paraules buides ja que no té sentit comparar els paràgrafs per exemple per
    // articles, determinants o pronoms febles
    var stopWords: Set[String] = Set()
    val stopwordsFile = "../files/stop_words/stop_words.txt"
    
    for (line <- Source.fromFile(stopwordsFile).getLines) {
        stopWords += line
    }
        
    /**
      * Main
      *
      * @param args Array[String]
      */
    def main(args: Array[String]): Unit = {

        // s'obtenen les linies i es netegen de caràcters extranys i accents
        var lines = Source.fromFile("../files/quijote/raw_quijote.txt").getLines.toArray.map(x => filterStopWords(x)).map(x => cleanStrings(x))

        // amb les línies es construeixen els paràgrafs
        val text = constructParagraphs(lines)

        // es construeix un nou fitxer on cada línia representa un paràgraf net d'accents i símbols
        new PrintWriter("../files/quijote/clean_quijote.txt", "UTF-8") {
            write(text.toString); close()
        }
    }

    /**
      * Filtra les paraules buides
      *
      * @param line: String
      */
    private def filterStopWords(line: String) : String = {

        // s'obté un array de la línia de text
        val arrayLine = line.split(" ")

        // es neteja l'array de paraules buides
        val cleanArrayLine = arrayLine.filterNot(word => stopWords.contains(word.toLowerCase))

        // es torna a reconstruir l'string i es retorna net
        cleanArrayLine.mkString(" ")

    }

    /**
      * Neteja les línies del text de caràcters extranys i accents
      *
      * @param line String
      * @return String
      */
    private def cleanStrings(line: String): String = {

        // es crea una hash de lletres accentuades i les seves substitutions
        val substitutions = Map('á' -> 'a', 'é' -> 'e', 'í' -> 'i', 'ó' -> 'o', 'ú' -> 'u', 'ñ' ->'n')

        // es converteix el text a minúscules
        var replacedLine = line.toLowerCase()

        // s'apliquen les substitucions dels accents
        for (char <- replacedLine) {
            if (substitutions.contains(char)) {
                replacedLine = replacedLine.replace(char, substitutions(char))

            }
        }

        // s'eliminen els caràcters extranys i es retorna la línia
        replacedLine.replaceAll("[^A-Za-z0-9]", " ")

    }

    /**
      * Construeix els paràgrafs a partir de les linies del text
      *
      * @param lines Array[String]
      * @return String
      */
    private def constructParagraphs(lines: Array[String]) : String = {

        // es crea l'string del paràgraf. Comença buit.
        var paragraphsArray: String = ""
        var paragraph = ""

        for (i <- lines.indices) {

            // es concatena cada línia del paràgraf al nou paràgraf.
            paragraph = paragraph.concat(lines(i)).trim.concat(" ")

            // es comprova que la línia següent pertanyi al mateix paràgraf.
            if (i < lines.length - 1 && lines(i + 1).isEmpty) {
                if (!paragraph.trim.isEmpty)
                    paragraphsArray = paragraphsArray.concat(paragraph.trim).concat("\n")
                paragraph = ""
            }

            // es concatena la última línia
            if (i == lines.length - 1) {
                paragraphsArray = paragraphsArray.concat(lines(i))
            }

        }

        // es netegen els dobles espais i es retorna el paràgraf
        paragraphsArray.replaceAll("  ", " ")
    }

}

