/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class BytesReferenceStreamInputTests extends AbstractStreamTests {

    private static final int BYTES_IN_MB = (int) ByteSizeValue.ofMb(1).getBytes();

    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        return bytesReference.streamInput();
    }

    public void testDeserializeBytesArray() throws IOException {
        deserializeBytesReference(randomIntBetween(0, BYTES_IN_MB - 1), BytesArray.class);
    }

    public void testDeserializePagedBytesReference() throws IOException {
        deserializeBytesReference(randomIntBetween(BYTES_IN_MB, 3 * BYTES_IN_MB), PagedBytesReference.class);
    }

    private void deserializeBytesReference(int length, Class<?> c) throws IOException {
        final byte[] bytes = new byte[length];
        random().nextBytes(bytes);
        final BytesReference bytesReference = new BytesArray(bytes);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeBytesReference(bytesReference);
        // TODO: We should randomly wrap with FilterStreamInput but it does not work
        final StreamInput input = getStreamInput(out.bytes());
        assertThat(input.readBytesReference(), instanceOf(c));
    }
}
