package org.example;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.nvidia.spark.RapidsUDF;
import morfologik.stemming.Dictionary;
import morfologik.stemming.DictionaryLookup;
import morfologik.stemming.WordData;
import org.apache.spark.sql.api.java.UDF1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GPUUDF implements RapidsUDF {
    private static GpuLemmatizer lemmatizer;

    static {
        lemmatizer = new GpuLemmatizer();
    }

//    @Override
//    public List<String> call(String text) throws Exception {
//        if (text == null || text.trim().isEmpty()) {
//            return List.of();  // Handle null/empty input gracefully
//        }
//
//        String[] words = TextHelper.splitToWords(text).toArray(new String[0]);
//        String[] lemmas = lemmatizer.lemmatize(words);
//
//        return Arrays.asList(lemmas);
//    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected 1 input column, got " + args.length);
        }

        ColumnVector input = args[0];
        if (!input.getType().equals(DType.STRING)) {
            throw new IllegalArgumentException("Input column must be of type STRING, got " + input.getType());
        }

        try (HostColumnVector hostInput = input.copyToHost()) {
            // Extract input strings from host column
            List<String> inputWords = new ArrayList<>(numRows);
            for (int i = 0; i < numRows; i++) {
                inputWords.add(hostInput.getJavaString(i));
            }

            // Call CUDA-backed JNI lemmatizer
            String[] result = lemmatizer.lemmatize(inputWords.toArray(new String[0]));

            // Build host output vector
            try (HostColumnVector.Builder hostOutputBuilder = HostColumnVector.builder(DType.STRING, numRows)) {
                for (String lemma : result) {
                    hostOutputBuilder.append(lemma);
                }

                try (HostColumnVector hostOutput = hostOutputBuilder.build()) {
                    // Copy to GPU
                    return hostOutput.copyToDevice();
                }
            }
        }
    }
}
