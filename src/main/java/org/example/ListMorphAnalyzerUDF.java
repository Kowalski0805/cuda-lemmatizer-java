package org.example;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.nvidia.spark.RapidsUDF;
import company.evo.jmorphy2.MorphAnalyzer;
import company.evo.jmorphy2.ParsedWord;
import org.apache.spark.sql.api.java.UDF1;

import java.util.ArrayList;
import java.util.List;

public class ListMorphAnalyzerUDF implements UDF1<String, List<List<String>>>, RapidsUDF {
    private static MorphAnalyzer analyzer;
//    private static SimpleTagger tagger;

    static {
        try {
            analyzer = new MorphAnalyzer.Builder().fileLoader(new JarFileLoader("company/evo/jmorphy2/uk/pymorphy2_dicts")).build();
//            tagger = new SimpleTagger(analyzer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<List<String>> call(String text) throws Exception {
        if (text == null) {
            return null;
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

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        // The CPU implementation takes a single string argument, so similarly
        // there should only be one column argument of type STRING.
        if (args.length != 1) {
            throw new IllegalArgumentException("Unexpected argument count: " + args.length);
        }
        ColumnVector input = args[0];
        if (numRows != input.getRowCount()) {
            throw new IllegalArgumentException("Expected " + numRows + " rows, received " + input.getRowCount());
        }
        if (!input.getType().equals(DType.STRING)) {
            throw new IllegalArgumentException("Argument type is not a string column: " +
                    input.getType());
        }

        // Create a list to store the normalized words
        List<String> normalizedWords = new ArrayList<>();

        try (HostColumnVector hostInput = input.copyToHost()) {
            for (int i = 0; i < numRows; i++) {
                String text = hostInput.getJavaString(i);
                if (text == null || text.trim().isEmpty()) {
                    normalizedWords.add(""); // Keep empty strings if the input is empty
                    continue;
                }

                // Split text into words (you could also use cudf tokenization here)
                String[] words = text.split("\\s+");

                List<String> normalized = new ArrayList<>();
                for (String word : words) {
                    normalized.add(normalizeWord(word, false));
                }

                // Join back into a single normalized sentence
                normalizedWords.add(String.join(" ", normalized));
            }
        }

        // Convert the list back into a cudf ColumnVector
        return ColumnVector.fromStrings(normalizedWords.toArray(new String[0]));
    }

    public static String normalizeWord(String word, Boolean deep) {
        String norm = analyzer.parse(word).get(0).normalForm;
        if (deep) {
            while (!norm.equals(word)) {
                word = norm;
                norm = analyzer.parse(word).get(0).normalForm;
            }
        }

        return norm;
    }

    public static List<String> normalizeSent(List<String> sentence, Boolean deep) {
        return sentence.stream()
                .map(word -> normalizeWord(word, deep))
                .toList();
    }

    public static List<String> parseSent(List<String> sentence) {
        return sentence.stream()
                .map(word -> analyzer.parse(word).get(0).toString())
                .toList();
    }

    public static List<ParsedWord> tokenizeSent(List<String> sentence) {
        return sentence.stream()
                .map(word -> analyzer.parse(word).get(0))
                .toList();
    }

    public static List<List<String>> normalizeText(List<List<String>> text, Boolean deep) {
        return text.stream()
                .map(sentence -> normalizeSent(sentence, deep))
                .toList();
    }
}
