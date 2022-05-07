val in = "input1.txt"
val inputfile = sc.textFile(in)
val counts = inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
counts.toDebugString
counts.cache()
counts.saveAsTextFile("output")

