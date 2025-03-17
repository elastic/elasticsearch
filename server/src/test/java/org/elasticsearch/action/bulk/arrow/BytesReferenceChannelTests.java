/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk.arrow;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;

public class BytesReferenceChannelTests extends ESTestCase {

    /**
     * Check iteration on the buffers of a composite byteref
     */
    public void testMultipleBuffers() throws IOException {

        var chunks = new ArrayList<BytesReference>();
        byte[] chunk = null;
        for (int i = 0; i < 0x100; i++) {
            if (i % 0x10 == 0) {
                chunk = new byte[0x10];
                chunks.add(new BytesArray(chunk));
            }
            chunk[i % 0x10] = (byte) i;
        }

        var bytesref = CompositeBytesReference.of(chunks.toArray(new BytesReference[0]));

        try (var channel = new BytesReferenceChannel(bytesref)) {
            var in = Channels.newInputStream(channel);

            for (int i = 0; i < 0x100; i++) {
                assertEquals(i, in.read());
            }

            assertEquals(-1, in.read());
        }
    }
}
