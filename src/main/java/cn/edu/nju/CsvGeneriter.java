package cn.edu.nju;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;


public class CsvGeneriter{
//define the nodes, edges map to print the csv file.
    static HashMap<String, Integer> nodes = new HashMap<String,Integer>();
    static HashMap<String, Double> edges = new HashMap<String,Double>();
    static HashMap<String, Double> fakeEdges = new HashMap<String,Double>();
    static HashMap<String, Integer> tags = new HashMap<String,Integer>();
    static HashMap<String, Double> prs = new HashMap<String,Double>();
//arg 0 for tag, 1 for edge , 2 for pr , 3 & 4 for output node & edge
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        
        if(args.length!=4){
            System.err.println("Usage: csv <pagerank> <relation> <tag>");
            System.exit(2);
        }
        System.out.println("generite begain");
        System.out.println(args[1]);
        String line;
        Scanner scPr = new Scanner(new File(args[1]),"UTF-8");
        while(scPr.hasNextLine()){
            line = scPr.nextLine();
            
            int index_t = line.indexOf("\t");
            int index_l = line.indexOf("[");
            int index_r = line.indexOf("]");

            String word = line.substring(0, index_t);
            double value = Double.parseDouble(line.substring(index_t+1,index_l));
            //System.out.println(word+":"+value);
            prs.put(word, value);
        }
        scPr.close();

        System.out.println(args[3]);
        Scanner scTag = new Scanner(new File(args[3]),"UTF-8");
        int nodecount =0;
        while(scTag.hasNextLine()){
            line = scTag.nextLine();
            StringTokenizer stWord=new StringTokenizer(line);
            String word = stWord.nextToken();
            int tag = Integer.parseInt(stWord.nextToken());
            tags.put(word, tag);
            nodes.put(word, nodecount);
            nodecount++;
        }
        scTag.close();
        
        System.out.println(args[2]);
        Scanner scEdge = new Scanner(new File(args[2]),"UTF-8");
        while(scEdge.hasNext()){
            line = scEdge.nextLine();

            int index_t = line.indexOf("\t");

            String pair = line.substring(0,index_t);
            Double value = Double.parseDouble(line.substring(index_t+1));

            StringTokenizer stPair = new StringTokenizer(pair,",");
            int node1No=0;
            int node2No=0;

            node1No = nodes.get(stPair.nextToken());
            node2No = nodes.get(stPair.nextToken());

            if(node1No>node2No){
                String edge = new String(node1No+","+node2No);
                edges.put(edge, value);
            }
        }
        scEdge.close();

        PrintWriter pr2 = new PrintWriter(new File("node.csv"),"UTF-8");
        Set<Entry<String,Integer>> set = tags.entrySet();
        Iterator<Entry<String,Integer>> it0= set.iterator();
        pr2.println("id,label,tag,pr");
        while(it0.hasNext()){
            Entry<String,Integer> entry = it0.next();
            String word = entry.getKey();
            int tag = entry.getValue();
            int nodeId = nodes.get(word);
            double value_pr = prs.get(word);
            pr2.println(nodeId+","+word+","+tag+","+value_pr);
        }
        pr2.close();

        PrintWriter pr3 = new PrintWriter(new File("edge.csv"));
        Set<Entry<String, Double>> set0 = edges.entrySet();
        Iterator<Entry<String, Double>> it1 = set0.iterator();
        int edgeCount =0;
        
        pr3.println("Source,Target,id,weight");
        while(it1.hasNext()){
            Entry<String, Double> entry=it1.next();
            String edge = entry.getKey();
            double weight = entry.getValue();
            pr3.println(edge+","+edgeCount+","+weight);
            ++edgeCount;
        }
        pr3.close();
        System.out.println("Processing sucess!");
    }
}
