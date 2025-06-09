package org.example;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import com.nvidia.spark.RapidsUDF;
import org.apache.spark.sql.api.java.UDF1;

public class GPUUDF implements UDF1<String, String>, RapidsUDF {
    private static GpuLemmatizer lemmatizer;

    static {
        lemmatizer = new GpuLemmatizer();
    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected 1 input column, got " + args.length);
        }

        ColumnVector input = args[0];
        if (!input.getType().equals(DType.STRING)) {
            throw new IllegalArgumentException("Input column must be of type STRING, got " + input.getType());
        }
        long inputPtr = input.getNativeView();
        System.out.println("inputPtr = " + inputPtr);
        long nativePtr = lemmatizer.lemmatize(inputPtr);
        System.out.println("nativePtr = " + nativePtr);
        return new ColumnVector(nativePtr);
    }

    @Override
    public String call(String s) throws Exception {
        return "";
    }
}
