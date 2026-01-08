/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.test.ESTestCase;

public class BytesRefsTests extends ESTestCase {

    public void testCodePointCountEmpty() {
        assertCodePointCount("");
    }

    public void testCodePointCountRandom() {
        for (int i = 0; i < 100_000; i++) {
            assertCodePointCount(randomUnicodeOfLengthBetween(0, 1000));
        }
    }

    public void assertCodePointCount(String s) {
        var bytes = new BytesRef(s);
        assertEquals(UnicodeUtil.codePointCount(bytes), BytesRefs.fastCodePointCount(bytes));
    }
}
