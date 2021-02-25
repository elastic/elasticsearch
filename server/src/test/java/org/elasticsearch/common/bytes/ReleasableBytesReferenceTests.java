/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        BytesReference delegate;
        String composite = "composite";
        String paged = "paged";
        String array = "array";
        String type = randomFrom(composite, paged, array);
        if (array.equals(type)) {
            final BytesStreamOutput out = new BytesStreamOutput(length);
            for (int i = 0; i < length; i++) {
                out.writeByte((byte) random().nextInt(1 << 8));
            }
            assertThat(length, equalTo(out.size()));
            BytesArray ref = new BytesArray(out.bytes().toBytesRef().bytes, 0, length);
            assertThat(length, equalTo(ref.length()));
            assertThat(ref.length(), Matchers.equalTo(length));
            delegate = ref;
        } else if (paged.equals(type)) {
            ByteArray byteArray = bigarrays.newByteArray(length);
            for (int i = 0; i < length; i++) {
                byteArray.set(i, (byte) random().nextInt(1 << 8));
            }
            assertThat(byteArray.size(), Matchers.equalTo((long) length));
            BytesReference ref = BytesReference.fromByteArray(byteArray, length);
            assertThat(ref.length(), Matchers.equalTo(length));
            delegate = ref;
        } else {
            assert composite.equals(type);
            List<BytesReference> referenceList = new ArrayList<>();
            for (int i = 0; i < length; ) {
                int remaining = length - i;
                int sliceLength = randomIntBetween(1, remaining);
                ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(sliceLength, bigarrays);
                for (int j = 0; j < sliceLength; j++) {
                    out.writeByte((byte) random().nextInt(1 << 8));
                }
                assertThat(sliceLength, equalTo(out.size()));
                referenceList.add(out.bytes());
                i += sliceLength;
            }
            BytesReference ref = CompositeBytesReference.of(referenceList.toArray(new BytesReference[0]));
            assertThat(length, equalTo(ref.length()));
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
