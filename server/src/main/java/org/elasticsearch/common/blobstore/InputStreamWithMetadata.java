/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class InputStreamWithMetadata implements Closeable {

    /**
     * Downloaded blob InputStream
     */
    private final InputStream inputStream;

    /**
     * Metadata of the downloaded blob
     */
    private final Map<String, String> metadata;

    public InputStream getInputStream() {
        return inputStream;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public InputStreamWithMetadata(InputStream inputStream, Map<String, String> metadata) {
        this.inputStream = inputStream;
        this.metadata = metadata;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
