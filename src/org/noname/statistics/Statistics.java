package org.noname.statistics;

import java.util.HashMap;
import java.util.Map;

public class Statistics {
    private final String url;
    private final Map<String, Integer> wordsMap;

    public Statistics (String url, String text) {
        this.url = url;
        String[] words = text.split("[ ,.!\\?\";:\\[\\]()\n\r\t]+");
        wordsMap = new HashMap<>();
        for (String word:words) {
            wordsMap.put(word,wordsMap.getOrDefault(word,0)+1);
        }
    }

    public Statistics (String url, Map<String, Integer> wordsMap) {
        this.url = url;
        this.wordsMap = wordsMap;
    }

    public void print() {
        System.out.printf("Statistics for url: %s\n---------------------\n", url);
        wordsMap.entrySet().stream()
                .sorted(Map.Entry.<String,Integer>comparingByValue()).forEach(entry -> System.out.println(entry.getKey() + " - " + entry.getValue()));
    }

    public String getUrl() {
        return url;
    }

    public Map<String, Integer> getWordsMap() {
        return wordsMap;
    }
}
