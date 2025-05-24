package org.example;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF1;
import java.util.ArrayList;
import java.util.List;

import company.evo.jmorphy2.MorphAnalyzer;

public class MorphAnalyzerUDF implements UDF1<String, String>, RapidsUDF {
    private static MorphAnalyzer analyzer;

    static {
        try {
            analyzer = new MorphAnalyzer.Builder().fileLoader(new JarFileLoader("company/evo/jmorphy2/uk/pymorphy2_dicts")).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String call(String word) throws Exception {
        if (word == null) {
            return null;
        }

        return normalizeWord(word, false);
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

        try (HostColumnVector hostInput = input.copyToHost()) {
            List<String> results = new ArrayList<>(numRows);

            for (int i = 0; i < numRows; i++) {
                String word = hostInput.getJavaString(i);
                results.add(word != null ? normalizeWord(word, false) : null);
            }

            // Convert the list of normalized words back into a cudf ColumnVector
            try (HostColumnVector.Builder hostOutputBuilder = HostColumnVector.builder(DType.STRING, numRows)) {
                for (String normalizedWord : results) {
                    hostOutputBuilder.append(normalizedWord);
                }
                try (HostColumnVector hostOutput = hostOutputBuilder.build()) {
                    return hostOutput.copyToDevice();
                }
            }
        }
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
}

// TODO: mental dump: try to run java cudf instead of jmorphy/morfologik; when finished with C++, try to connect it to java udf
