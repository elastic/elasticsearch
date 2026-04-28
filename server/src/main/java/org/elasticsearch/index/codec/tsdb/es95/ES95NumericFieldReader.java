/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.NumericFieldReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesBlockReader;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;

import java.io.IOException;

/**
 * {@link NumericFieldReader} implementation for the ES95 TSDB format.
 *
 * <p>{@link #readFieldEntry} delegates to {@link TSDBDocValuesBlockReader} and additionally
 * reads the {@link FieldDescriptor} from the field's metadata header so the correct
 * pipeline decoder can be reconstructed at read time.
 *
 * <p>{@link #decoder()} returns a {@link Decoder} backed by the pipeline identified by the
 * {@link FieldDescriptor}, falling back to the provided fallback decoder for ordinal-range
 * and single-ordinal fields that bypass pipeline encoding.
 */
final class ES95NumericFieldReader implements NumericFieldReader {

    private static final TSDBDocValuesBlockReader BLOCK_READER = new TSDBDocValuesBlockReader();

    private final NumericCodecFactory numericCodecFactory;
    private final Decoder fallbackDecoder;
    private PipelineDescriptor pipelineDescriptor;

    ES95NumericFieldReader(final NumericCodecFactory numericCodecFactory, final Decoder fallbackDecoder) {
        this.numericCodecFactory = numericCodecFactory;
        this.fallbackDecoder = fallbackDecoder;
    }

    @Override
    public void readFieldEntry(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
        BLOCK_READER.readFieldEntry(meta, entry, numericBlockShift, m -> pipelineDescriptor = FieldDescriptor.read(m));
    }

    @Override
    public Decoder decoder() {
        if (pipelineDescriptor != null) {
            final NumericDecoder decoder = numericCodecFactory.createDecoder(pipelineDescriptor);
            final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
            return (input, values, count) -> blockDecoder.decode(values, count, input);
        }
        return fallbackDecoder;
    }

    /**
     * Default {@link FallbackDecoderFactory} implementation that decodes blocks using
     * {@link TSDBDocValuesEncoder}, the pre-pipeline encoding used for ordinal-range
     * and single-ordinal fields.
     *
     * @param numericBlockSize number of values per block
     * @return a decoder backed by {@link TSDBDocValuesEncoder}
     */
    static Decoder defaultFallbackDecoder(int numericBlockSize) {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(numericBlockSize);
        return (input, values, count) -> encoder.decode(input, values);
    }
}
