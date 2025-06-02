/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatVariableSkipIntervalTests;

/** Tests ES819TSDBDocValuesFormat with custom skipper interval size. */
public class ES819TSDBDocValuesFormatVariableSkipIntervalTests extends ES87TSDBDocValuesFormatVariableSkipIntervalTests {

    @Override
    protected Codec getCodec() {
        // small interval size to test with many intervals
        return TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat(random().nextInt(4, 16), random().nextBoolean()));
    }

    public void testSkipIndexIntervalSize() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ES819TSDBDocValuesFormat(random().nextInt(Integer.MIN_VALUE, 2), random().nextBoolean())
        );
        assertTrue(ex.getMessage().contains("skipIndexIntervalSize must be > 1"));
    }
}
