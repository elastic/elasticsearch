/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;

/**
 * Selects the appropriate TSDB doc values format based on index settings and index version.
 */
public final class TSDBDocValuesFormatSelector {

    private TSDBDocValuesFormatSelector() {}

    /**
     * Selects the TSDB doc values format for the given index settings.
     *
     * @param indexSettings        the index settings to base the selection on
     * @param fieldContextResolver bridge from the mapper layer that supplies the
     *                             per-field {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldContext}
     *                             used by the ES95 pipeline resolver, or {@code null}
     *                             when mapper metadata is not available
     * @return the selected doc values format
     */
    public static DocValuesFormat select(final IndexSettings indexSettings, @Nullable final FieldContextResolver fieldContextResolver) {
        final IndexVersion indexCreatedVersion = indexSettings.getIndexVersionCreated();
        final boolean useLargeNumericBlockSize = indexSettings.isUseTimeSeriesDocValuesFormatLargeNumericBlockSize();
        final boolean useLargeBinaryBlockSize = indexSettings.isUseTimeSeriesDocValuesFormatLargeBinaryBlockSize();
        final boolean writePartitions = indexSettings.getMode().isTsdb()
            && TsidBuilder.useSingleBytePrefixLayout(indexCreatedVersion)
            && indexCreatedVersion.onOrAfter(IndexVersions.WRITE_TSID_PREFIX_PARTITION);

        if (useES95(indexSettings)) {
            return ES95TSDBDocValuesFormatFactory.create(
                useLargeNumericBlockSize,
                useLargeBinaryBlockSize,
                writePartitions,
                fieldContextResolver
            );
        }
        return ES819TSDBDocValuesFormatFactory.createDocValuesFormat(
            indexCreatedVersion,
            useLargeNumericBlockSize,
            useLargeBinaryBlockSize,
            writePartitions
        );
    }

    static boolean useES95(final IndexSettings indexSettings) {
        return indexSettings.getMode().isTsdb()
            && indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG)
            && indexSettings.isTimeSeriesEs95CodecEnabled();
    }
}
