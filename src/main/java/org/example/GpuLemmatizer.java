package org.example;

public class GpuLemmatizer {
    static {
        System.loadLibrary("lemmatizer"); // .so or .dll
    }

    public native long lemmatize(long words);
}
