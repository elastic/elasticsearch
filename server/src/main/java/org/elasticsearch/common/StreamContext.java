/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.io.InputStreamContainer;

import java.io.IOException;

public class StreamContext {

    private final CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> streamSupplier;
    private final long partSize;
    private final long lastPartSize;
    private final int numberOfParts;

    /**
     * Construct a new StreamProvider object
     *
     * @param streamSupplier A {@link CheckedTriFunction} that will be called with the <code>partNumber</code>, <code>partSize</code> and <code>position</code> in the stream
     * @param partSize Size of all parts apart from the last one
     * @param lastPartSize Size of the last part
     * @param numberOfParts Total number of parts
     */
    public StreamContext(
        CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> streamSupplier,
        long partSize,
        long lastPartSize,
        int numberOfParts
    ) {
        this.streamSupplier = streamSupplier;
        this.partSize = partSize;
        this.lastPartSize = lastPartSize;
        this.numberOfParts = numberOfParts;
    }

    /**
     * Copy constructor for overriding class
     */
    protected StreamContext(StreamContext streamContext) {
        this.streamSupplier = streamContext.streamSupplier;
        this.partSize = streamContext.partSize;
        this.numberOfParts = streamContext.numberOfParts;
        this.lastPartSize = streamContext.lastPartSize;
    }

    /**
     * Vendor plugins can use this method to create new streams only when they are required for processing
     * New streams won't be created till this method is called with the specific <code>partNumber</code>
     * It is the responsibility of caller to ensure that stream is properly closed after consumption
     * otherwise it can leak resources.
     *
     * @param partNumber The index of the part
     * @return A stream reference to the part requested
     */
    public InputStreamContainer provideStream(int partNumber) throws IOException {
        long position = partSize * partNumber;
        long size = (partNumber == numberOfParts - 1) ? lastPartSize : partSize;
        return streamSupplier.apply(partNumber, size, position);
    }

    /**
     * @return The number of parts in which this file is supposed to be uploaded
     */
    public int getNumberOfParts() {
        return numberOfParts;
    }
}
