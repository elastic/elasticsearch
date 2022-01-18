/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.internal.io.Streams;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class StreamsUtils {

    public static String copyToStringFromClasspath(ClassLoader classLoader, String path) throws IOException {
        InputStream is = classLoader.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath with class loader [" + classLoader + "]");
        }
        return org.elasticsearch.common.io.Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static String copyToStringFromClasspath(String path) throws IOException {
        InputStream is = Streams.class.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }
        return org.elasticsearch.common.io.Streams.copyToString(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    public static byte[] copyToBytesFromClasspath(String path) throws IOException {
        try (InputStream is = Streams.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
            }
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                Streams.copy(is, out);
                return BytesReference.toBytes(out.bytes());
            }
        }
    }

}
