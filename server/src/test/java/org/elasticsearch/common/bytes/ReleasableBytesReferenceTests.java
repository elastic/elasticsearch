/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.ByteArray;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

public class ReleasableBytesReferenceTests extends AbstractBytesReferenceTestCase {

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return newBytesReferenceWithOffsetOfZero(length);
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        return newBytesReference(randomByteArrayOfLength(length));
    }

    @Override
    protected BytesReference newBytesReference(byte[] content) throws IOException {
        BytesReference delegate;
        String composite = "composite";
        String paged = "paged";
        String array = "array";
        String type = randomFrom(composite, paged, array);
        if (array.equals(type)) {
            final BytesStreamOutput out = new BytesStreamOutput(content.length);
            out.writeBytes(content, 0, content.length);
            assertThat(content.length, equalTo(out.size()));
            BytesArray ref = new BytesArray(out.bytes().toBytesRef().bytes, 0, content.length);
            assertThat(content.length, equalTo(ref.length()));
            assertThat(ref.length(), Matchers.equalTo(content.length));
            delegate = ref;
        } else if (paged.equals(type)) {
            ByteArray byteArray = bigarrays.newByteArray(content.length);
            byteArray.set(0, content, 0, content.length);
            assertThat(byteArray.size(), Matchers.equalTo((long) content.length));
            BytesReference ref = BytesReference.fromByteArray(byteArray, content.length);
            assertThat(ref.length(), Matchers.equalTo(content.length));
            delegate = ref;
        } else {
            assert composite.equals(type);
            List<BytesReference> referenceList = new ArrayList<>();
            for (int i = 0; i < content.length;) {
                int remaining = content.length - i;
                int sliceLength = randomIntBetween(1, remaining);
                ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(sliceLength, bigarrays);
                out.writeBytes(content, content.length - remaining, sliceLength);
                assertThat(sliceLength, equalTo(out.size()));
                referenceList.add(out.bytes());
                i += sliceLength;
            }
            BytesReference ref = CompositeBytesReference.of(referenceList.toArray(new BytesReference[0]));
            assertThat(content.length, equalTo(ref.length()));
            delegate = ref;
        }
        return ReleasableBytesReference.wrap(delegate);
    }

    @Override
    public void testToBytesRefSharedPage() throws IOException {
        // CompositeBytesReference doesn't share pages
    }

    @Override
    public void testSliceArrayOffset() throws IOException {
        // the assertions in this test only work on no-composite buffers
    }

    @Override
    public void testSliceToBytesRef() throws IOException {
        // CompositeBytesReference shifts offsets
    }
}
