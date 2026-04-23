/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.SegmentReadState;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.TSDBNumericBlockCodec;
import org.elasticsearch.index.codec.tsdb.TSDBOrdinalBlockCodec;

import java.io.IOException;

/**
 * Doc values producer for the ES819 TSDB format.
 */
final class ES819TSDBDocValuesProducer extends AbstractTSDBDocValuesProducer {

    ES819TSDBDocValuesProducer(
        final SegmentReadState state,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Decoder docOffsetsDecoder
    ) throws IOException {
        super(
            state,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension,
            formatConfig,
            docOffsetsDecoder,
            new TSDBNumericBlockCodec(),
            new TSDBOrdinalBlockCodec()
        );
    }

    private ES819TSDBDocValuesProducer(final ES819TSDBDocValuesProducer original) {
        super(original);
    }

    @Override
    protected AbstractTSDBDocValuesProducer createMergeInstance() {
        return new ES819TSDBDocValuesProducer(this);
    }
}
