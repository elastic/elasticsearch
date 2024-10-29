/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteBufferStreamInputTests extends AbstractStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        final BytesRef bytesRef = bytesReference.toBytesRef();
        return new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
    }

    public void testReadVLongNegative() throws IOException {
        for (int i = 0; i < 1024; i++) {
            long write = randomNegativeLong();
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeVLongNoCheck(write);
            long read = getStreamInput(out.bytes()).readVLong();
            assertEquals(write, read);
        }
    }

    public void testReadVLongBounds() throws IOException {
        long write = Long.MAX_VALUE;
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeVLongNoCheck(write);
        long read = getStreamInput(out.bytes()).readVLong();
        assertEquals(write, read);

        write = Long.MIN_VALUE;
        out = new BytesStreamOutput();
        out.writeVLongNoCheck(write);
        read = getStreamInput(out.bytes()).readVLong();
        assertEquals(write, read);
    }

    public void testReadVIntNegative() throws IOException {
        for (int i = 0; i < 1024; i++) {
            int write = randomNegativeInt();
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeVInt(write);
            int read = getStreamInput(out.bytes()).readVInt();
            assertEquals(write, read);
        }
    }

    public void testReadVIntBounds() throws IOException {
        int write = Integer.MAX_VALUE;
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeVInt(write);
        long read = getStreamInput(out.bytes()).readVInt();
        assertEquals(write, read);

        write = Integer.MIN_VALUE;
        out = new BytesStreamOutput();
        out.writeVInt(write);
        read = getStreamInput(out.bytes()).readVInt();
        assertEquals(write, read);
    }
}
