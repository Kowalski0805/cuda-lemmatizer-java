package org.example;

import company.evo.jmorphy2.FileLoader;
import java.io.IOException;
import java.io.InputStream;

public class JarFileLoader extends FileLoader {
    private final String basePath;

    public JarFileLoader(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public InputStream newStream(String filename) throws IOException {
        String fullPath = basePath + "/" + filename;

        // Try loading from JAR
        InputStream in = getClass().getClassLoader().getResourceAsStream(fullPath);
        if (in == null) {
            throw new IOException("Resource not found: " + fullPath);
        }
        return in;
    }
}

