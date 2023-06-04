/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.ejerciciotema3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 *
 * @author desarrollo
 */
public class main {    
    SparkConf conf = new SparkConf().setAppName("ANALISIS COVID-19").setMaster("local[*]");
    /*Inciso 1*/    
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
    
    Dataset<Row> df = spark.read().option("header", true).option("inferSchema", "true").csv("/home/desarrollo/Escritorio/big data/Tema 3: Procesamiento distribuido/Ejercicio/covid_autopesquisa.csv");
    df.printShema();
    df.show(10);
    
    /*Inciso 2*/   
    Dataset<Row> ausentes= df.na().drop();
    
    /*Inciso 3*/ 
    Long filas = df.count();
    int columnas = df.columns().length;
    System.out.println("cantidad fila: "+ filas +" Cantidad de columnas: "+columnas);
        
    /*Inciso 4*/ 
    df.createOrReplaceTempView("pesquisa");
    Dataset<Row> sqlDF = spark.sql("SELECT Nombre, Edad, Sexo, Policlinico FROM pesquisa ORDER BY Edad");
    sqlDF.show(50);
    
    /*Inciso 5*/ 
    Dataset<Row> contactos = spark.sql("SELECT Nombre FROM pesquisa Where Contacto con Positivo = 'Si'");
    contactos.show();
    
    /*Inciso 6*/
    contactos.count().show();
    
    /*Inciso 7*/
    Dataset<Row> conEnfermedad = spark.sql("SELECT * FROM pesquisa where Hipertension = 'Si' or Insuficiencia Cardiaca = 'Si' or Enfermedad Coronaria = 'Si' or Cancer = 'Si' or Diabetes = 'Si' or VIH = 'Si'");
    conEnfermedad.show();

    /*Inciso 8*/
    Dataset<Row> promedio = spark.sql("SELECT AVG(Edad) FROM pesquisa where Diabetes = 'Si' and Sexo = 'F'");
    promedio.show();
    
    /*Inciso 9*/
    Dataset<Row> sintoma = spark.sql("SELECT * FROM pesquisa where Fiebre = 'Si' or Dolor de Garganta = 'Si' or Dolor de Cabeza = 'Si' or Tos = 'Si' or Falta de Aire = 'Si'");
    sintoma.show();
    
    /*Inciso 10*/
    Dataset<Row> viajo = spark.sql("(SELECT count(*) as cantidad, Sexo FROM pesquisa where Han viajado = 'Si' and Sexo = 'F') union (SELECT count(*) as cantidad, Sexo FROM pesquisa where Han viajado = 'Si' and Sexo = 'M')");
    viajo.show();
    
}
 