/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import java.io.InputStream;

public class InputStreamContainer {

    private final InputStream inputStream;
    private final long contentLength;
    private final long offset;

    /**
     * Construct a new stream object
     *
     * @param inputStream   The input stream that is to be encapsulated
     * @param contentLength The total content length that is to be read from the stream
     */
    public InputStreamContainer(InputStream inputStream, long contentLength, long offset) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
        this.offset = offset;
    }

    /**
     * @return The input stream this object is reading from
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * @return The total length of the content that has to be read from this stream
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * @return offset of the source content.
     */
    public long getOffset() {
        return offset;
    }
}
