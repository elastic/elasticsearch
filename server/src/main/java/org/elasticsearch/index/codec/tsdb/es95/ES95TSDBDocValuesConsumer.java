/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.NumericBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserverFactory;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;

import java.io.IOException;

/**
 * Doc values consumer for the ES95 TSDB format. Delegates all shared wire-format logic
 * to {@link AbstractTSDBDocValuesConsumer} with ES95-specific pipeline-based numeric
 * encoding via {@link NumericBlockCodec} and the shared {@link OrdinalBlockCodec} for ordinals.
 */
final class ES95TSDBDocValuesConsumer extends AbstractTSDBDocValuesConsumer {

    ES95TSDBDocValuesConsumer(
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
        final SortedFieldObserverFactory sortedFieldObserverFactory,
        final NumericBlockCodec numericCodec,
        final OrdinalBlockCodec ordinalCodec
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
            numericCodec,
            ordinalCodec
        );
    }
}
