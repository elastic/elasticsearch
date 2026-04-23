/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * {@link OrdinalFieldReader} implementation that reads the ordinal stream of sorted and
 * sorted-set fields from the TSDB block layout.
 *
 * <p>{@link #readFieldEntry} delegates to the shared metadata parsing in
 * {@link TSDBDocValuesBlockReader}. {@link #decoder()} returns the {@link Decoder} supplied at
 * construction time, which the iteration code drives during ordinal access.
 */
public final class TSDBOrdinalFieldReader implements OrdinalFieldReader {

    private static final TSDBDocValuesBlockReader BLOCK_READER = new TSDBDocValuesBlockReader();

    private final Decoder decoder;

    /**
     * @param decoder per-block decoder that supplies the codec-specific ordinal decoding
     */
    public TSDBOrdinalFieldReader(final Decoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public void readFieldEntry(final IndexInput meta, final AbstractTSDBDocValuesProducer.NumericEntry e, int numericBlockShift)
        throws IOException {
        BLOCK_READER.readFieldEntry(meta, e, numericBlockShift);
    }

    @Override
    public Decoder decoder() {
        return decoder;
    }
}
