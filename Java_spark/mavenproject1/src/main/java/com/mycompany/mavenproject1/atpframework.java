/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenproject1;
// Import package
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 *
 * @author s.henrique20
 */
public class atpframework {
            
    public static void main (String[] args){
    
    System.out.println("Início");
    
    // Create config spark
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PraticaSpark");
    
    // Create context using spark
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    // Dataset
    JavaRDD<String> arquivo = sc.textFile(
        "/home/Disciplinas/Frameworks/spark/ocorrencias_criminais_sample.csv"
    );

    
   // Quesão 1º - Quantidade de crimes por ano
    JavaRDD<String> anoRDD = arquivo.map(s -> 
        { 
            String [] campos = s.split(";");
            return campos[2];
        });
    System.out.println("##### QUESTÃO 1 #####");
    System.out.println(anoRDD.countByValue());
    
    
    // Questão 2º - Quantidade de crimes por ano que sejam do tipo "NARCOTICS"? 
    JavaRDD<String> tiponarcoticsRDD = arquivo.filter(s -> 
        { 
            String [] campos = s.split(";");
            String tipo = campos[4];
            return tipo.equalsIgnoreCase("NARCOTICS");
        });
    
    JavaRDD<String> tiponarcoticsanoRDD = tiponarcoticsRDD.map(s -> 
        { 
            String [] campos = s.split(";");
            return campos[2];
        }); 
    System.out.println("##### QUESTÃO 2 #####");
    System.out.println(tiponarcoticsanoRDD.countByValue());
    
    
    // Questão 3º - Quantidade de crimes por ano que sejam do tipo "narcotics" 
    // e tenham ocorridos em dia pares?     
    JavaRDD<String> diasparesRDD = tiponarcoticsRDD.filter(s -> 
        { 
            String [] campos = s.split(";");
            Integer diaspares = Integer.parseInt(campos[0]);
        return diaspares % 2 == 0;
        });
    
    JavaRDD<String> diasparesanoRDD = diasparesRDD.map(s -> 
        { 
            String [] campos = s.split(";");
            return campos[2];
        });  
    
    System.out.println("###### QUESTÃO 3 ######");
    System.out.println(diasparesanoRDD.countByValue());
    
    
    // Questão 4º - Mês com maior ocorrência de crimes?
    JavaRDD<String> mesRDD = arquivo.map(s -> 
        { 
            String [] campos = s.split(";");
            return campos[1];
        });
    

    System.out.println("##### QUESTÃO 4 #####");
    System.out.println(mesRDD.countByValue());
    

    // Questão 5º - Mês com maior média de ocorrência de crimes?
    JavaRDD<String> mesmediaRDD = arquivo.map(s -> 
        { 
            String [] campos = s.split(";");
            String mesano = campos[1] + "-" + campos[2];
            return mesano;
        });
   
    System.out.println("###### QUESTÃO 5 ######");
    Map mesmediaList = mesmediaRDD.countByValue();
    System.out.println(mesmediaList);
    

    // Questão 6º - Mês por ano com a maior ocorrência de crimes?
    JavaRDD<String> mesanoRDD = arquivo.map(s -> 
        { 
            String [] campos = s.split(";");
            String mesano = campos[1] + "-" + campos[2];
            return mesano;
        });
          
    System.out.println("##### QUESTÃO 6  ######");
    System.out.println(mesanoRDD.countByValue());
    
    
    // Questão 7º - Mês com a maior ocorrência de crimes do tipo 
    // deceptive practice?
    JavaRDD<String> tipodeceptiveRDD = arquivo.filter(s -> 
        { 
            String [] campos = s.split(";");
            String tipo = campos[4];
            return tipo.equalsIgnoreCase("DECEPTIVE PRACTICE");
        });
    
    JavaRDD<String> tipodeceptivemesRDD = tipodeceptiveRDD.map(s -> 
        { 
            String [] campos = s.split(";");
            return campos[1];
        }); 
    System.out.println("###### QUESTÃO 7 #####");
    System.out.println(tipodeceptivemesRDD.countByValue());
    
    

    // Questão 8º -Dia do ano com a maior ocorrência de crimes?
    JavaRDD<String> diaanoRDD = arquivo.map(s -> 
        { 
            String [] campos = s.split(";");
            return campos[0];
        });
              
    System.out.println("##### QUESTÃO 8 #####");
    System.out.println(diaanoRDD.countByValue());
    
   
    }

}
