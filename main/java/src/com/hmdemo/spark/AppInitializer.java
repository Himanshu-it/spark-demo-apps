package com.hmdemo.spark;

public class AppInitializer {
    private static SparkApp sparkApp;
    public static void main(String[] args) {
        sparkApp = new WordCounterApp();
        sparkApp.process();
    }
}
