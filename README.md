# Map-Reduce_Optimization

In this project, I have improved the performance of my code developed in https://github.com/sohanbadade/Map-Reduce-simple-histogram in two different ways: 

1) using a combiner and 
2) using in-mapper combining. 

I have written two Hadoop Map-Reduce jobs in the same file project2/src/main/java/Histogram.java. 

Each one of this Map-Reduce jobs will read the same input file but will produce output to a different output directory: in this Java main program, both Map-Reduce jobs will read args[0] as the input file with the pixels (which is pixels-small.txt or pixels-large.txt). The first Map-Reduce job will write on the output directory args[1] (which is output) while the second Map-Reduce job will write on the output directory args[1]+"2" (which is output2). 

If you want to look at the results of the second Map-Reduce job in distributed mode, you can add the following statement in project2/histogram.distr.run:
