/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.AbstractStreamTests;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.zip.CRC32;

public class BufferedChecksumStreamInputTests extends AbstractStreamTests {

    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) {
        return new BufferedChecksumStreamInput(StreamInput.wrap(BytesReference.toBytes(bytesReference)), "test");
    }

    public void testChecksum() throws IOException {
        int bytesSize = randomIntBetween(512, 2048);
        byte[] bytes = randomByteArrayOfLength(bytesSize);
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);

        try (BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(StreamInput.wrap(bytes), "test")) {
            int read = input.read(new byte[bytesSize]);
            assertEquals(bytesSize, read);
            assertEquals(-1, input.read());
            assertEquals(crc32.getValue(), input.getChecksum());
        }
    }
}
