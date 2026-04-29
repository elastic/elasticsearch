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
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;

public class ES819DocValuesDuelTests extends AbstractTSDBDocValuesDuelTests {

    @Override
    protected DocValuesFormat baselineFormat() {
        return new Lucene90DocValuesFormat();
    }

    @Override
    protected DocValuesFormat contenderFormat() {
        return randomBoolean()
            ? new ES819Version3TSDBDocValuesFormat(
                randomIntBetween(1, 4096),
                randomIntBetween(1, 512),
                random().nextBoolean(),
                TSDBDocValuesTestUtil.randomBinaryCompressionMode(),
                random().nextBoolean(),
                TSDBDocValuesTestUtil.randomNumericBlockSize(),
                randomBoolean()
            )
            : new TestES87TSDBDocValuesFormat();
    }
}
