package org.csv;
import au.com.bytecode.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Vaggelis on 3/7/2016.
 */
class WorkerThread implements Runnable {
    private String img;
    private String[] newArray;
    private ConcurrentHashMap<String,String[]> csvMap;
    public WorkerThread(String img, String[] newArray,ConcurrentHashMap csvMap){
        this.img=img;
        this.csvMap = csvMap;
        this.newArray = newArray;
    }
    private boolean IsIn(String[] newArray){
        boolean found = false;
        for(String img : this.csvMap.keySet()){
            if(Arrays.equals(newArray,this.csvMap.get(img))) found = true;
        }
        return found;
    }
    public void run() {
//        System.out.printf("Running %s\n",this.img);
        if(!IsIn(this.newArray)){
            this.csvMap.put(img, newArray);
        }else {
            System.out.println("Duplication!");
        }
    }
    private void processmessage() {
        try {  Thread.sleep(2000);  } catch (InterruptedException e) { e.printStackTrace(); }
    }
}
public class CsvProcess {
    ConcurrentHashMap<String,String[]> csvMap;
    HashMap <String,String[]> csvMapRev;
    HashMap<String[],String> tempCsvMap;
    private boolean IsIn(String[] newArray){
        boolean found = false;
        for(String img : this.csvMap.keySet()){
            if(Arrays.equals(newArray,this.csvMap.get(img))) found = true;
        }
        return found;
    }
    CsvProcess(String path, boolean threaded){
        this.csvMap = new ConcurrentHashMap<String, String[]>();
        if(threaded){
            try {
                CSVReader reader = new CSVReader(new FileReader(path));
                Collection<Future<?>> futures = new LinkedList<Future<?>>();
                Integer parallel = 8;
                ExecutorService executor = Executors.newFixedThreadPool(parallel);
                String [] nextLine;
                float completed = 1;
                float total = 234842;
                while ((nextLine = reader.readNext()) != null) {
                    if(completed % parallel != 0){
                        Runnable worker = new WorkerThread(nextLine[0],Arrays.copyOfRange(nextLine, 1, 1001),this.csvMap);
//                        executor.execute(worker);//calling execute method of ExecutorService
                        futures.add(executor.submit(worker));
                    }
                    else{
//                    System.out.println("Finished all threads... restarting");
                        for (Future<?> future:futures) {
                            future.get();
                        }
                        futures.clear();
                    }
                    System.out.printf("\r%.0f%%",(100 * completed) / total);
                    completed++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else{
            try {
                CSVReader reader = new CSVReader(new FileReader(path));
                String [] nextLine;
                float completed = 1;
                float total = 234842;
                while ((nextLine = reader.readNext()) != null) {
                    String[] newArray = Arrays.copyOfRange(nextLine, 1, 1001);
                    if(!IsIn(newArray)){
                        this.csvMap.put(nextLine[0], newArray);
                        System.out.printf("\r%.0f%%",(100 * completed) / total);
                        completed++;
                    }else {
                        System.out.println("Duplication!");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    CsvProcess(String path){
        this.csvMapRev = new HashMap<String, String[]>();
        this.tempCsvMap = new HashMap<String[],String>();
        try {
            CSVReader reader = new CSVReader(new FileReader(path));
            String [] nextLine;
            System.out.println("Reading");
            float completed = 1;
            float total = 234842;
            while ((nextLine = reader.readNext()) != null) {
                String[] newArray = Arrays.copyOfRange(nextLine, 1, 1001);
                this.csvMapRev.put(nextLine[0], newArray);
                System.out.printf("\r%.0f%%",(100 * completed) / total);
                completed++;
            }
            System.out.println("Reversing\n\n");
            completed = 1;
            for(Map.Entry<String, String[]> entry : this.csvMapRev.entrySet()){
                this.tempCsvMap.put(entry.getValue(), entry.getKey());
                System.out.printf("\r%.0f%%",(100 * completed) / total);
                completed++;
            }
            this.csvMapRev.clear();
            System.out.println("Rewriting\n\n");
            completed = 1;
            for(Map.Entry<String[], String> entry : this.tempCsvMap.entrySet()){
                this.csvMapRev.put(entry.getValue(), entry.getKey());
                System.out.printf("\r%.0f%%",(100 * completed) / total);
                completed++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        CsvProcess csvProcess = new CsvProcess("C:\\Users\\Vaggelis\\PycharmProjects\\DeepNet\\train_photos.csv", true);
//        CsvProcess csvProcess = new CsvProcess("C:\\Users\\Vaggelis\\PycharmProjects\\DeepNet\\train_photos.csv");
//        CsvProcess csvProcess = new CsvProcess("C:\\Users\\Vaggelis\\PycharmProjects\\DeepNet\\train_photos.csv", false);
    }
}