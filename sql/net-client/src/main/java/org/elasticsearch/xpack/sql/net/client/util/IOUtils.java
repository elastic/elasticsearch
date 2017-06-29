/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.net.client.util;

import java.io.IOException;
import java.io.InputStream;

public class IOUtils {
    public static Bytes asBytes(InputStream input) throws IOException {
        BasicByteArrayOutputStream bos = new BasicByteArrayOutputStream(input.available());
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
        return bos.bytesArray();
    }
}
