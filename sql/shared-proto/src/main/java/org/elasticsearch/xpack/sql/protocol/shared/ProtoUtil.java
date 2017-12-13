/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.IOException;

public class ProtoUtil {
    private static final int MAX_ARRAY_SIZE = 5 * 1024 * 1024 * 1024;

    public static int readArraySize(DataInput in) throws IOException {
        int length = in.readInt();
        if (length > MAX_ARRAY_SIZE) {
            throw new IOException("array size unbelievably long [" + length + "]");
        }
        return length;
    }
}
