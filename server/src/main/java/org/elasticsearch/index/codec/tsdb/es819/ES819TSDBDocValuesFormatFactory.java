/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

/**
 * Factory class for creating instances of {@link DocValuesFormat} tailored for time-series
 * use cases in relation to specific index versions and numeric block size preferences.
 */
public final class ES819TSDBDocValuesFormatFactory {

    static final DocValuesFormat ES_819_2_TSDB_DOC_VALUES_FORMAT = ES819TSDBDocValuesFormat.getInstance(false);
    static final DocValuesFormat ES_819_2_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK = ES819TSDBDocValuesFormat.getInstance(true);

    static final DocValuesFormat ES_819_3_TSDB_DOC_VALUES_FORMAT = new ES819Version3TSDBDocValuesFormat(false, false, false);
    static final DocValuesFormat ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        false,
        true,
        false
    );
    static final DocValuesFormat ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        false,
        false
    );

    static final DocValuesFormat ES_819_4_TSDB_DOC_VALUES_FORMAT = new ES819Version3TSDBDocValuesFormat(false, false, true);
    static final DocValuesFormat ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        false,
        true,
        true
    );
    static final DocValuesFormat ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        false,
        true
    );

    private ES819TSDBDocValuesFormatFactory() {}

    /**
     * Creates and returns a DocValuesFormat instance based on the specified index version
     * and whether to use a large numeric block size.
     *
     * @param indexCreatedVersion      the version of the index being created, which determines
     *                                 the applicable DocValuesFormat version.
     * @param useLargeNumericBlockSize a boolean flag indicating whether to use a large numeric block size.
     * @param useLargeBinaryBlockSize  a boolean flag indicating whether to use a large binary block size.
     * @param writePrefixPartitions    a boolean flag indicating whether to write the prefix partition for the primary sort field
     * @return the appropriate DocValuesFormat instance based on the index version and block size selection.
     */
    public static DocValuesFormat createDocValuesFormat(
        IndexVersion indexCreatedVersion,
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePrefixPartitions
    ) {
        if (writePrefixPartitions) {
            if (useLargeBinaryBlockSize) {
                return ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK;
            }
            return useLargeNumericBlockSize ? ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_4_TSDB_DOC_VALUES_FORMAT;
        } else if (indexCreatedVersion.onOrAfter(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3)) {
            if (useLargeBinaryBlockSize) {
                // At this stage, we don't need large numeric blocks if large binary block is requested:
                assert useLargeNumericBlockSize == false;
                return ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK;
            }
            return useLargeNumericBlockSize ? ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_3_TSDB_DOC_VALUES_FORMAT;
        } else {
            return useLargeNumericBlockSize ? ES_819_2_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_2_TSDB_DOC_VALUES_FORMAT;
        }
    }
}
