// Databricks notebook source
/*************************************************************************************************************
* PRÀCTICA 3 DE BASES DE DADES AVANÇADES
*
* Pau Sanchez i Guillem Rabionet
*
**************************************************************************************************************
* Aquest programa carrega el fitxer clean_quijote.txt. Aquest fitxer s'ha d'haver generat de la forma que
* s'indica a la memòria de la pràctica. Tots els passos a seguir estan correctament indicats a la memòria.
*
* Nota: aquest programa treu el resultat imprimit per pantalla. El mateix programa fent en Intellij
* enlloc d'imprimir el resultat per pantalla, crea un fitxer TSV i el desa a disc. S'ha fet així degut
* a que no s'ha trobat la manera de desar fitxers a disc des de DataBricks. S'explicat tot a la memòria,
* així com tots els passos a seguir.
***************************************************************************************************************/

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import scala.util.Random

// es llegeix el fitxer 
val quijote = sc.textFile("/FileStore/tables/clean_quijote.txt")

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
for (paragraph <- input.take(10)) {
  
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
      for (i <- 0 to wordVector.size - 1) {
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

// es printegen els 10 primers paràgrafs vectoritzats
// això és el que caldria convertir a fitxer però com que des de databricks
// resulta molt complicat desar un fitxer al disc dur, s'ha realitzat l'exemple sencer
// des del projecte d'IntellijIdea adjuntat a la pràctica.
// Tal i com s'explica a la memòria executant el projecte (que és exactament aquest codi però enlloc
// de fer print desa l'string a disc com a fitxer TSV) es pot obtenir el fitxer TSV per tensor flow.
// Si us plau, cal seguir les instruccions de la memòria!
println(stringToTensorflowFile)
