/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

public class Crc32CTests extends ESTestCase {

    public void testChecksum() {
        assertEquals("AAAAAA==", Crc32C.checksum(BytesArray.EMPTY));
        assertEquals("B8uf9g==", Crc32C.checksum(new BytesArray(new byte[100])));
        final byte[] ascending = new byte[32];
        for (int i = 0; i < 32; i++) {
            ascending[i] = (byte) i;
        }
        assertEquals("Rt15Tg==", Crc32C.checksum(new BytesArray(ascending)));
    }
}
