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
        int pageIndex = randomIntBetween(0, 100_000);
        EsqlCursor.StorageType storageType = randomFrom(EsqlCursor.StorageType.values());

        EsqlCursor original = new EsqlCursor(storageType, id, pageIndex);
        String encoded = original.encode();
        EsqlCursor decoded = EsqlCursor.decode(encoded);

        assertEquals(storageType, decoded.storageType());
        assertEquals(id, decoded.cursorId());
        assertEquals(pageIndex, decoded.pageIndex());
    }

    public void testDifferentCursorsProduceDifferentTokens() {
        EsqlCursor c1 = new EsqlCursor(EsqlCursor.StorageType.INDEX, "abc", 0);
        EsqlCursor c2 = new EsqlCursor(EsqlCursor.StorageType.INDEX, "abc", 10);
        assertNotEquals(c1.encode(), c2.encode());
    }

    public void testDifferentStorageTypesProduceDifferentTokens() {
        EsqlCursor c1 = new EsqlCursor(EsqlCursor.StorageType.INDEX, "abc", 0);
        EsqlCursor c2 = new EsqlCursor(EsqlCursor.StorageType.DISK, "abc", 0);
        assertNotEquals(c1.encode(), c2.encode());
    }

    public void testDecodeInvalidTokenThrows() {
        expectThrows(Exception.class, () -> EsqlCursor.decode("not-a-valid-token!!!"));
    }

    public void testStorageTypeFromString() {
        assertEquals(EsqlCursor.StorageType.INDEX, EsqlCursor.StorageType.fromString("index"));
        assertEquals(EsqlCursor.StorageType.DISK, EsqlCursor.StorageType.fromString("disk"));
        assertEquals(EsqlCursor.StorageType.INDEX, EsqlCursor.StorageType.fromString("INDEX"));
        expectThrows(IllegalArgumentException.class, () -> EsqlCursor.StorageType.fromString("unknown"));
    }
}
