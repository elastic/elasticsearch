/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;

public class EsqlCursorTests extends ESTestCase {

    public void testRoundTrip() {
        String id = randomAlphaOfLength(22);
        int offset = randomIntBetween(0, 100_000);
        int pageSize = randomIntBetween(1, 10_000);

        EsqlCursor original = new EsqlCursor(id, offset, pageSize);
        String encoded = original.encode();
        EsqlCursor decoded = EsqlCursor.decode(encoded);

        assertEquals(id, decoded.cursorId());
        assertEquals(offset, decoded.nextRowOffset());
        assertEquals(pageSize, decoded.pageSize());
    }

    public void testDifferentCursorsProduceDifferentTokens() {
        EsqlCursor c1 = new EsqlCursor("abc", 0, 10);
        EsqlCursor c2 = new EsqlCursor("abc", 10, 10);
        assertNotEquals(c1.encode(), c2.encode());
    }

    public void testDecodeInvalidTokenThrows() {
        expectThrows(Exception.class, () -> EsqlCursor.decode("not-a-valid-token!!!"));
    }
}
