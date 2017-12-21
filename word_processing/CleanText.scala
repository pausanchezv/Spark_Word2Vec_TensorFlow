import scala.io.Source
import java.io._

/**
  * Objecte de funcions que proporciona utilitats diverses per la pràctica
  */
object CleanText {

    // es tenen en compte les paraules buides ja que no té sentit comparar els paràgrafs per exemple per
    // articles, determinants o pronoms febles
    var stopWords: Set[String] = Set()
    val stopwordsFile = "../files/stop_words.txt"
    
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
        var lines = Source.fromFile("../files/raw_quijote.txt").getLines.toArray.map(x => filterStopWords(x)).map(x => cleanStrings(x))

        // amb les línies es construeixen els paràgrafs
        val text = constructParagraphs(lines)

        // es construeix un nou fitxer on cada línia representa un paràgraf net d'accents i símbols
        new PrintWriter("../files/clean_quijote.txt", "UTF-8") {
            write(text.toString); close()
        }
    }

    /**
      * Filtra les paraules buides
      *
      * @param line: String
      */
    private def filterStopWords(line: String) : String = {

        val arrayLine = line.split(" ")
        val cleanArrayLine = arrayLine.filterNot(word => stopWords.contains(word.toLowerCase))
        cleanArrayLine.mkString(" ")

    }

    /**
      * Neteja les línies del text de caràcters extranys i accents
      *
      * @param line String
      * @return String
      */
    private def cleanStrings(line: String): String = {

        val substitutions = Map('á' -> 'a', 'é' -> 'e', 'í' -> 'i', 'ó' -> 'o', 'ú' -> 'u', 'ñ' ->'n')

        var replacedLine = line.toLowerCase()

        for (char <- replacedLine) {
            if (substitutions.contains(char)) {
                replacedLine = replacedLine.replace(char, substitutions(char))

            }
        }
        replacedLine.replaceAll("[^A-Za-z0-9]", " ")

    }


    /**
      * Construeix els paràgrafs a partir de les linies del text
      *
      * @param lines Array[String]
      * @return String
      */
    private def constructParagraphs(lines: Array[String]) : String = {

        var paragraphsArray: String = ""
        var paragraph = ""

        for (i <- lines.indices) {

            paragraph = paragraph.concat(lines(i)).trim.concat(" ")

            if (i < lines.length - 1 && lines(i + 1).isEmpty) {
                if (!paragraph.trim.isEmpty)
                    paragraphsArray = paragraphsArray.concat(paragraph.trim).concat("\n")
                paragraph = ""
            }

            if (i == lines.length - 1) {
                paragraphsArray = paragraphsArray.concat(lines(i))
            }

        }
        paragraphsArray.replaceAll("  ", " ")
    }

}

