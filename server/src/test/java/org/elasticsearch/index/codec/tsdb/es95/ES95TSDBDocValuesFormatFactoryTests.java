/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.test.ESTestCase;

public class ES95TSDBDocValuesFormatFactoryTests extends ESTestCase {

    public void testCreateReturnsFreshInstanceOnEveryCall() {
        final DocValuesFormat a = ES95TSDBDocValuesFormatFactory.create(true, false, true, null);
        final DocValuesFormat b = ES95TSDBDocValuesFormatFactory.create(true, false, true, null);
        assertNotSame(a, b);
    }

    public void testCreateReturnsValidInstanceForEachParameterCombination() {
        for (int n = 0; n < 2; n++) {
            for (int b = 0; b < 2; b++) {
                for (int p = 0; p < 2; p++) {
                    assertNotNull(ES95TSDBDocValuesFormatFactory.create(n == 1, b == 1, p == 1, null));
                }
            }
        }
    }
}
