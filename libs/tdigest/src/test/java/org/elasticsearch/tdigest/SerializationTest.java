/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SerializationTest {
    @Test
    public void mergingDigestSerDes() {
        final TDigest out = MergingDigest.createDigest(100);
        out.add(42.5);
        out.add(1);
        out.add(24.0);

        final ByteBuffer output = ByteBuffer.allocate(out.smallByteSize());
        out.asSmallBytes(output);

        ByteBuffer input = ByteBuffer.wrap(output.array());
        try {
            TDigest m = MergingDigest.fromBytes(input);
            for (double q = 0; q <= 1; q+=0.001) {
                assertEquals(m.quantile(q), out.quantile(q), 0);
            }
            Iterator<Centroid> ix = m.centroids().iterator();
            for (Centroid centroid : out.centroids()) {
                assertTrue(ix.hasNext());
                Centroid c = ix.next();
                assertEquals(centroid.mean(), c.mean(), 0);
                assertEquals(centroid.count(), c.count(), 0);
            }
            assertFalse(ix.hasNext());
        } catch (BufferUnderflowException e) {
            System.out.println("WTF?");
        }

        input = ByteBuffer.wrap(output.array());
        final TDigest in = MergingDigest.fromBytes(input);
        assertEquals(out.quantile(0.95), in.quantile(0.95), 0.001);
    }
}
