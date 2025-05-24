package org.example;

import org.apache.spark.sql.api.java.UDF1;
import morfologik.stemming.Dictionary;
import morfologik.stemming.DictionaryLookup;
import morfologik.stemming.WordData;
import java.io.IOException;
import java.util.List;

public class MorfologikUDF implements UDF1<String, List<List<String>>> {
    private static Dictionary dictionary;
    private static DictionaryLookup lookup;

    static {
        try {
            dictionary = Dictionary.read(MorfologikUDF.class.getClassLoader().getResource("org/languagetool/resource/uk/ukrainian.dict"));
            lookup = new DictionaryLookup(dictionary);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Morfologik dictionary", e);
        }
    }

    // Thread-local DictionaryLookup instances
    private static final ThreadLocal<DictionaryLookup> lookupThreadLocal =
            ThreadLocal.withInitial(() -> {
                try {
                    return new DictionaryLookup(dictionary);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize DictionaryLookup", e);
                }
            });

    @Override
    public List<List<String>> call(String text) throws Exception {
        if (text == null || text.trim().isEmpty()) {
            return List.of();  // Return empty list instead of null
        }

        List<String> sentences = TextHelper.splitToSentences(text, false);
        List<List<String>> words = sentences.stream()
                .map(TextHelper::splitToWords)
                .toList();
        List<List<String>> normWords = normalizeText(words, false);
        return normWords.stream()
                .map(TextHelper::filterStopWords)
                .toList();
    }

    public static String normalizeWord(String word, Boolean deep) {
        if (word == null) {
            return word;
        }

//        word = word.replaceAll("[^\\p{L}\\p{N}]", ""); // Keep only letters and numbers
//
//        if (word.isEmpty()) {
//            return word;
//        }

        try {
//            DictionaryLookup lookup = new DictionaryLookup(dictionary);
            DictionaryLookup lookup = lookupThreadLocal.get();
//            List<WordData> results = lookup.lookup(word);
//            if (results.isEmpty()) {
//                return word;
//            }
            String norm = lookup.lookup(word).get(0).getStem().toString();
            if (deep) {
//                int depth = 0;  // Prevent infinite loops
                while (!norm.equals(word)) {
                    word = norm;
//                    results = lookup.lookup(word);
//                    if (results.isEmpty()) {
//                        break;
//                    }
                    norm = lookup.lookup(word).get(0).getStem().toString();
//                    depth++;
                }
            }

            return norm;
        } catch (Exception e) {
            return word;
        }
    }

    public static List<String> normalizeSent(List<String> sentence, Boolean deep) {
        return sentence.stream()
                .map(word -> normalizeWord(word, deep))
                .toList();
    }

    public static List<String> parseSent(List<String> sentence) {
        return sentence.stream()
                .map(word -> lookup.lookup(word).get(0).toString())
                .toList();
    }

    public static List<WordData> tokenizeSent(List<String> sentence) {
        return sentence.stream()
                .map(word -> lookup.lookup(word).get(0))
                .toList();
    }

    public static List<List<String>> normalizeText(List<List<String>> text, Boolean deep) {
        return text.stream()
                .map(sentence -> normalizeSent(sentence, deep))
                .toList();
    }
}
