/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserverFactory;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.TSDBNumericBlockCodec;
import org.elasticsearch.index.codec.tsdb.TSDBOrdinalBlockCodec;

import java.io.IOException;

/**
 * Doc values consumer for the ES819 TSDB format.
 */
final class ES819TSDBDocValuesConsumer extends AbstractTSDBDocValuesConsumer {

    ES819TSDBDocValuesConsumer(
        final SegmentWriteState state,
        boolean enableOptimizedMerge,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final String skipCodec,
        final String skipExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Encoder docOffsetsEncoder,
        final SortedFieldObserverFactory sortedFieldObserverFactory
    ) throws IOException {
        super(
            state,
            enableOptimizedMerge,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension,
            skipCodec,
            skipExtension,
            formatConfig,
            docOffsetsEncoder,
            sortedFieldObserverFactory,
            new TSDBNumericBlockCodec(),
            new TSDBOrdinalBlockCodec()
        );
    }
}
