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

public class TSDBDocValuesFormatFactoryTests extends ESTestCase {

    public void testVersion3WithoutLargeBlockSize() {
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3, false)
        );
    }

    public void testVersion3WithLargeBlockSize() {
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3, true)
        );
    }

    public void testVersionAfterVersion3() {
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), false)
        );
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            TSDBDocValuesFormatFactory.createDocValuesFormat(IndexVersion.current(), true)
        );
    }

    public void testPreVersion3WithoutLargeBlockSize() {
        IndexVersion preV3 = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3);
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_2_TSDB_DOC_VALUES_FORMAT,
            TSDBDocValuesFormatFactory.createDocValuesFormat(preV3, false)
        );
    }

    public void testPreVersion3WithLargeBlockSize() {
        IndexVersion preV3 = IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DOC_VALUES_FORMAT_VERSION_3);
        assertSame(
            TSDBDocValuesFormatFactory.ES_819_2_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK,
            TSDBDocValuesFormatFactory.createDocValuesFormat(preV3, true)
        );
    }
}
