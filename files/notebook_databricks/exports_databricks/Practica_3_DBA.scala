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

// el fitxer d'entrada és el quijote net d'accents i signes que ja s'ha passat pel programa
// de neteja (la memòria explica com generar-lo)
val input_file = "/FileStore/tables/clean_quijote.txt"

// fitxer de sortida '.tsv' per visualitzar a tesorflow
val output_file = "/FileStore/tables/quijote_to_tensorflow.tsv"

// es llegeix el fitxer 
val quijote = sc.textFile(input_file)

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
// el qual després es guardarà com a fitxer ja amb el format admès per tensorflow web.
// tal i com explica la memòria es visualitzaran molt millor 1000 paràgrafs que els 5000 que té el llibre.
// si es volen visualitzar tots els paràgrafs només cal canviar '.take(1000)' per '.collect'
for (paragraph <- input.take(1000)) {
  
  // cada paràgraf tindrà un vector de característiques format pel conjunt dels vectors de les paraules
  // vectoritzades de tot el paràgraf
  var pVector: Array[String] = Array()
  
  // es comptarà la longitud de tots els vectors
  var pCount = 0
  
  // es recorren les paraules de cada paràgraf
  for (word <- paragraph) {
  
    // s'aplica la vectorització mitjançant el mètode 'transform'
    // per tal de fer-ho cal que la paraula es trobi al vocabulari del model, el bloc
    // intent/captura ens ajudarà a gestionar-ho
    try {
      
      // vectorització de cada paraula
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
  // 100 primers valors corresponents a la primera paraula. D'aquesta manera s'agafen
  // 100 valors abitraris corresponents a les diferents paraules del paràgraf.
  var pList = util.Random.shuffle(pVector.toList)
  pList = pList.slice(0, 100)
  
  // es concatenen els valors a l'string que aniran precedits del text del paràgraf
  for (value <- pList) {
    stringToTensorflowFile = stringToTensorflowFile.concat("\t").concat(value.toString)
  }
  
  // es concatena un salt de línia al final de cada vectorització de paràgraf
  if (pCount > 0) {
    stringToTensorflowFile = stringToTensorflowFile.concat("\n")
  }
}

// s'elimina el fitxer de sortida del dbfs si existeix
dbutils.fs.rm(output_file)

// es crea el fitxer de sortida tsv omplert amb l'string de sortida
dbutils.fs.put(output_file, stringToTensorflowFile)

/*
 ********* LLEGIR AIXÔ SI US PLAU **********

Es pot comprovar que el '.tsv' s'ha generat correctament executant la següent instrucció: 
display(dbutils.fs.ls("/FileStore/tables"))

Ara és possible descarregar-se el fitxer copiant la segëunt URL, enganxant-la a la barra del navegador i executant-la
Alerta, però, amb el paràmetre o=1519882038357773, ja que pertany a la sessió de DataBricks de Pau Sanchez. Per descarregar
el fitxer el lector ha d'alçar la mirada a la URL del notebook, canviar el paràmetre pel seu i llavors executar la nova
URL amb el seu paràmetre personal:

https://community.cloud.databricks.com/files/tables/quijote_to_tensorflow.tsv?o=1519882038357773

Això funciona correctament, ara bé, per si el lector tingués algun tipus de problema per descarregar-se el fitxer, aquest
fitxer ja es troba generat a l'entrega. La memòria n'explica la localització exacta.

A més, per si això no fós suficient, s'ha preparat un projecte d'Intellij que també genera el fitxer de sortida '.tsv'
*/

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))
