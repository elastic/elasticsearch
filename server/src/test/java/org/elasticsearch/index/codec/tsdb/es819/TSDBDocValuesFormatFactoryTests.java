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

public class TSDBDocValuesFormatFactoryTests extends ESTestCase {

    public void testVersion3() {
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3, false, false)
        );
    }

    public void testVersion3WithLargeNumericBlockSize() {
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3, true, false)
        );
    }

    public void testVersion3WithLargeBinaryBlockSize() {
        var actual = (ES819Version3TSDBDocValuesFormat) TSDBDocValuesFormatFactory.createDocValuesFormat(
            IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3,
            false,
            true
        );
        assertSame(TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK, actual);
        assertThat(actual.blockBytesThreshold, equalTo(1024 * 1024));
        assertThat(actual.blockCountThreshold, equalTo(32768));
    }

    public void testVersionAfterVersion3() {
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false, false)
        );
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), true, false)
        );
    }

    public void testPreVersion3() {
        IndexVersion preV3 = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3);
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_2_TSDB_DOC_VALUES_FORMAT,
            TSDBDocValuesFormatFactory.createDocValuesFormat(preV3, false, false)
        );
    }

    public void testPreVersion3WithLargeNumericBlockSize() {
        IndexVersion preV3 = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3);
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_2_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            TSDBDocValuesFormatFactory.createDocValuesFormat(preV3, true, false)
        );
    }
}
