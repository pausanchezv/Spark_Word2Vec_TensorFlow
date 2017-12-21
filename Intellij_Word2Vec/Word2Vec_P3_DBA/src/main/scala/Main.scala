import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import java.io._

object Main {

    /**
      * Configuració d'Spark
      */
    val conf: SparkConf = new SparkConf().setAppName("Pràctica 3 DBA").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder.getOrCreate()
    sc.setLogLevel("WARN")

    def main(args: Array[String]): Unit = {


        // es llegeix el fitxer
        val quijote = sc.textFile("../../files/clean_quijote.txt")

        // s'obtenen els tokens (paraules de cada paràgraf)
        val tokens = quijote.map(x => x.split("""\W+"""))

        // es preparen les dades com a 'WrappedArrays' que és el format admès pel
        // constructor de la funció 'fit' del word2vec
        val input = tokens.map(x => x.toSeq)

        // es crea l'objecte word2vec
        val word2vec = new Word2Vec()

        // es desenvolupa el model per entrenar mitjançant el mètode 'fit'
        val model = word2vec.fit(input)

        // es declara l'string que contindrà les dades vectoritzades i que serà convertit
        // a un fitxer 'tsv' un cop estigui plè
        var stringToTensorflowFile = ""

        // es recorren els paràgrafs de l'input ** Aqui es prepara el fitxer '.tsv' com a string
        // el qual després es guardarà com a fitxer ja amb el format admès per tensorflow web
        for (paragraph <- input.take(1000)) {

            // cada paràgraf tindrà un vector que serà el conjunt dels vectors de les paraules
            // vectoritzades de tot el paràgraf
            var pVector: Array[String] = Array()

            // es comptarà la longitud de tots els vectors
            var pCount = 0

            // es recorren les paraules de cada paràgraf
            for (word <- paragraph) {

                // s'aplica la vectorització mitjançanet em mètode 'transform'
                // per tal de fer-ho cal que la paraula es trobi al vocabulari del model
                try {
                    val wordVector = model.transform(word)

                    // s'afegeixen els valors de la vectorització al vector de característiques del paràgraf
                    for (i <- 0 until wordVector.size) {
                        pVector = pVector :+ wordVector(i).toString
                        pCount += 1
                    }
                }

                // si no s'hi troba, simplement deixem córrer la iteració
                catch {
                    case e: Exception => 0.0
                }

            }

            // primer es concatena el text del paràgraf
            if (pCount > 0) {
                stringToTensorflowFile = stringToTensorflowFile.concat("'").concat(paragraph.mkString(" ").concat("'"))
            }

            // es barreja el vector del paràgraf de manera que els valors de les diferents
            // paraules que conté queden barrejats. Si no es fés així, agafaria sempre els
            // 100 primers valors corresponents a la primera paraula. D'aquesta amera s'agafen
            // 100 valors abitraris corresponents a les diferents paraules del paràgraf.
            var pList = util.Random.shuffle(pVector.toList)
            pList = pList.slice(0, 100)

            // es concatenen els valors a l'string que aniran precedits del text del paràgraf
            for (value <- pList) {
                stringToTensorflowFile = stringToTensorflowFile.concat("\t").concat(value.toString)
            }

            // es concatena un salt de línia al final de cada representació
            if (pCount > 0) {
                stringToTensorflowFile = stringToTensorflowFile.concat("\n")
            }
        }

        //println(stringToTensorflowFile)

        new PrintWriter("../..//files/quijote_to_tensorflow.tsv") {
            write(stringToTensorflowFile)
            close()
        }

    }

}
