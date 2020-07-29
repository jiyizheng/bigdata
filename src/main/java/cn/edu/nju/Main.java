package cn.edu.nju;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println(args);
        if (args[0].equals("proprocessing"))
            ProProcessing.main(args);
        else if (args[0].equals("cooccurrence"))
            Cooccurrence.main(args);
        else if (args[0].equals("normalization"))
            Normalization.main(args);
        else if (args[0].equals("pagerank"))
            PageRank.main(args);
        else if (args[0].equals("lpa"))
            LabelPropagation.main(args);
        else if (args[0].equals("csv"))
            CsvGeneriter.main(args);
        else {
            System.err.println("command is incomprehensible");
        }
            
    }
}