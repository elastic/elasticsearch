/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A helper class for writing state to a native process
 */
public final class StateToProcessWriterHelper {

    private StateToProcessWriterHelper() {}

    public static void writeStateToStream(BytesReference source, OutputStream stream) throws IOException {
        // The source bytes are already UTF-8. The C++ process wants UTF-8, so we
        // can avoid converting to a Java String only to convert back again.
        int length = source.length();
        // There's a complication that the source can already have trailing 0 bytes
        while (length > 0 && source.get(length - 1) == 0) {
            --length;
        }
        source.slice(0, length).writeTo(stream);
        // This is dictated by the JSON parser on the C++ side; it treats a '\0' as the character
        // that separates distinct JSON documents, and this is what we need because we're
        // sending multiple JSON documents via the same named pipe.
        stream.write(0);
    }
}
