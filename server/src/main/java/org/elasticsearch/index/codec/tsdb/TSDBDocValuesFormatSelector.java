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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;

/**
 * Selects the appropriate TSDB doc values format based on index settings,
 * feature flags, and index version.
 */
public final class TSDBDocValuesFormatSelector {

    private TSDBDocValuesFormatSelector() {}

    /**
     * Selects the TSDB doc values format for the given index settings.
     *
     * @param indexSettings the index settings to base the selection on
     * @return the selected doc values format
     */
    public static DocValuesFormat select(final IndexSettings indexSettings) {
        final IndexVersion indexCreatedVersion = indexSettings.getIndexVersionCreated();
        final boolean useLargeNumericBlockSize = indexSettings.isUseTimeSeriesDocValuesFormatLargeNumericBlockSize();
        final boolean useLargeBinaryBlockSize = indexSettings.isUseTimeSeriesDocValuesFormatLargeBinaryBlockSize();
        final boolean writePartitions = indexSettings.getMode() == IndexMode.TIME_SERIES
            && TsidBuilder.useSingleBytePrefixLayout(indexCreatedVersion)
            && indexCreatedVersion.onOrAfter(IndexVersions.WRITE_TSID_PREFIX_PARTITION);

        if (useES95(indexSettings)) {
            return ES95TSDBDocValuesFormatFactory.createDocValuesFormat(useLargeNumericBlockSize, useLargeBinaryBlockSize, writePartitions);
        }
        return ES819TSDBDocValuesFormatFactory.createDocValuesFormat(
            indexCreatedVersion,
            useLargeNumericBlockSize,
            useLargeBinaryBlockSize,
            writePartitions
        );
    }

    static boolean useES95(final IndexSettings indexSettings) {
        return indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.ES95_TSDB_CODEC_FEATURE_FLAG)
            && indexSettings.isTimeSeriesEs95CodecEnabled();
    }
}
