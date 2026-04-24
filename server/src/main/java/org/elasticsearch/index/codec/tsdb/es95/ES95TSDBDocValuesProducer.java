/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.SegmentReadState;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;

import java.io.IOException;

/**
 * Doc values producer for the ES95 TSDB format. Delegates all shared wire-format reading logic to
 * {@link AbstractTSDBDocValuesProducer} with ES95-specific pipeline-based numeric decoding via
 * {@link NumericBlockCodec} and the shared {@link OrdinalBlockCodec} for ordinals.
 */
final class ES95TSDBDocValuesProducer extends AbstractTSDBDocValuesProducer {

    ES95TSDBDocValuesProducer(
        final SegmentReadState state,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final String skipCodec,
        final String skipExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Decoder docOffsetsDecoder,
        final NumericBlockCodec numericCodec,
        final OrdinalBlockCodec ordinalCodec
    ) throws IOException {
        super(
            state,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension,
            skipCodec,
            skipExtension,
            formatConfig,
            docOffsetsDecoder,
            numericCodec,
            ordinalCodec
        );
    }

    private ES95TSDBDocValuesProducer(final ES95TSDBDocValuesProducer original) {
        super(original);
    }

    @Override
    protected AbstractTSDBDocValuesProducer createMergeInstance() {
        return new ES95TSDBDocValuesProducer(this);
    }
}
