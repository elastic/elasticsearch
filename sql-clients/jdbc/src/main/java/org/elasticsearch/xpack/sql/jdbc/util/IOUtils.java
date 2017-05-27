/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;

public abstract class IOUtils {

    public static Properties propsFromString(String source) {
        Properties copy = new Properties();
        if (source != null) {
            try {
                copy.load(new StringReader(source));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return copy;
    }

    public static void close(Closeable closable) {
        if (closable != null) {
            try {
                closable.close();
            } catch (IOException e) {
                // silently ignore
            }
        }
    }


    public static String asString(InputStream in) throws IOException {
        return asBytes(in).toString();
    }

    public static BytesArray asBytes(InputStream in) throws IOException {
        BytesArray ba = unwrapStreamBuffer(in);
        if (ba != null) {
            return ba;
        }
        return asBytes(new BytesArray(in.available()), in);
    }

    public static BytesArray asBytes(BytesArray ba, InputStream input) throws IOException {
        BytesArray buf = unwrapStreamBuffer(input);
        if (buf != null) {
            ba.bytes(buf);
            return ba;
        }

        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(ba);
        byte[] buffer = new byte[1024];
        int read = 0;
        try (InputStream in = input) {
            while ((read = in.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
        } finally {
            // non needed but used to avoid the warnings
            bos.close();
        }
        return bos.bytes();
    }

    private static BytesArray unwrapStreamBuffer(InputStream in) {
        if (in instanceof FastByteArrayInputStream) {
            return ((FastByteArrayInputStream) in).data;
        }

        return null;
    }
}