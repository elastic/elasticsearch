/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import static org.hamcrest.Matchers.equalTo;

public class ES819TSDBDocValuesFormatFactoryTests extends ESTestCase {

    public void testVersion3() {
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(
                IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3,
                false,
                false,
                false
            )
        );
    }

    public void testVersion3WithLargeNumericBlockSize() {
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3, true, false, false)
        );
    }

    public void testVersion3WithLargeBinaryBlockSize() {
        var actual = (ES819Version3TSDBDocValuesFormat) ES819TSDBDocValuesFormatFactory.createDocValuesFormat(
            IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3,
            false,
            true,
            false
        );
        assertSame(ES819TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK, actual);
        assertThat(actual.formatConfig.blockBytesThreshold(), equalTo(512 * 1024));
        assertThat(actual.formatConfig.blockCountThreshold(), equalTo(8096));
    }

    public void testVersion3WithLargeNumericAndBinaryBlockSize() {
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_AND_BINARY_BLOCK,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3, true, true, false)
        );
    }

    public void testVersionAfterVersion3() {
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false, false, false)
        );
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), true, false, false)
        );
    }

    public void testPreVersion3() {
        IndexVersion preV3 = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3);
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_2_TSDB_DOC_VALUES_FORMAT,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(preV3, false, false, false)
        );
    }

    public void testPreVersion3WithLargeNumericBlockSize() {
        IndexVersion preV3 = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3);
        assertSame(
            ES819TSDBDocValuesFormatFactory.ES_819_2_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            ES819TSDBDocValuesFormatFactory.createDocValuesFormat(preV3, true, false, false)
        );
    }
}
