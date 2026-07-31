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
    static final DocValuesFormat ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        true,
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
    static final DocValuesFormat ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        true,
        true
    );

    // Variants with columnar layout for flattened ._keyed fields (writeColumnarFlattenedBinary=true).
    // Indexed by [writePrefixPartitions][largeNumericBlock][largeBinaryBlock].
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT = new ES819Version3TSDBDocValuesFormat(false, false, false, true);
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        false,
        true,
        false,
        true
    );
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        false,
        false,
        true
    );
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        true,
        false,
        true
    );
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS = new ES819Version3TSDBDocValuesFormat(
        false,
        false,
        true,
        true
    );
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS_LARGE_BINARY_BLOCK = new ES819Version3TSDBDocValuesFormat(
        false,
        true,
        true,
        true
    );
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS_LARGE_NUMERIC_BLOCK = new ES819Version3TSDBDocValuesFormat(
        true,
        false,
        true,
        true
    );
    static final DocValuesFormat ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS_LARGE_NUMERIC_AND_BINARY_BLOCK =
        new ES819Version3TSDBDocValuesFormat(true, true, true, true);

    private ES819TSDBDocValuesFormatFactory() {}

    /**
     * Back-compat overload that sets {@code writeColumnarFlattenedBinary=false}.
     * Prefer the 5-argument form for new call sites that should opt in to the columnar layout.
     */
    public static DocValuesFormat createDocValuesFormat(
        IndexVersion indexCreatedVersion,
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePrefixPartitions
    ) {
        return createDocValuesFormat(indexCreatedVersion, useLargeNumericBlockSize, useLargeBinaryBlockSize, writePrefixPartitions, false);
    }

    /**
     * Creates and returns a DocValuesFormat instance based on the specified index version
     * and whether to use a large numeric block size.
     *
     * @param indexCreatedVersion           the version of the index being created, which determines
     *                                      the applicable DocValuesFormat version.
     * @param useLargeNumericBlockSize      a boolean flag indicating whether to use a large numeric block size.
     * @param useLargeBinaryBlockSize       a boolean flag indicating whether to use a large binary block size.
     * @param writePrefixPartitions         a boolean flag indicating whether to write the prefix partition for the primary sort field
     * @param writeColumnarFlattenedBinary  a boolean flag indicating whether to write flattened {@code ._keyed} binary doc values
     *                                      using the columnar block layout
     * @return the appropriate DocValuesFormat instance based on the index version and block size selection.
     */
    public static DocValuesFormat createDocValuesFormat(
        IndexVersion indexCreatedVersion,
        boolean useLargeNumericBlockSize,
        boolean useLargeBinaryBlockSize,
        boolean writePrefixPartitions,
        boolean writeColumnarFlattenedBinary
    ) {
        if (writeColumnarFlattenedBinary) {
            if (writePrefixPartitions) {
                if (useLargeNumericBlockSize && useLargeBinaryBlockSize) {
                    return ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS_LARGE_NUMERIC_AND_BINARY_BLOCK;
                } else if (useLargeBinaryBlockSize) {
                    return ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS_LARGE_BINARY_BLOCK;
                }
                return useLargeNumericBlockSize
                    ? ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS_LARGE_NUMERIC_BLOCK
                    : ES_819_5_TSDB_DOC_VALUES_FORMAT_WITH_PARTITIONS;
            } else {
                if (useLargeNumericBlockSize && useLargeBinaryBlockSize) {
                    return ES_819_5_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK;
                } else if (useLargeBinaryBlockSize) {
                    return ES_819_5_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK;
                }
                return useLargeNumericBlockSize ? ES_819_5_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_5_TSDB_DOC_VALUES_FORMAT;
            }
        } else if (writePrefixPartitions) {
            if (useLargeNumericBlockSize && useLargeBinaryBlockSize) {
                return ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK;
            } else if (useLargeBinaryBlockSize) {
                return ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK;
            }
            return useLargeNumericBlockSize ? ES_819_4_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_4_TSDB_DOC_VALUES_FORMAT;
        } else if (indexCreatedVersion.onOrAfter(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3)) {
            if (useLargeNumericBlockSize && useLargeBinaryBlockSize) {
                return ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK;
            } else if (useLargeBinaryBlockSize) {
                return ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK;
            }
            return useLargeNumericBlockSize ? ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_3_TSDB_DOC_VALUES_FORMAT;
        } else {
            return useLargeNumericBlockSize ? ES_819_2_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK : ES_819_2_TSDB_DOC_VALUES_FORMAT;
        }
    }
}
