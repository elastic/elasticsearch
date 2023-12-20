/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.common.io.stream.AbstractStreamTests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ReleasableBytesReferenceStreamInputTests extends AbstractStreamTests {
    private final List<ReleasableBytesReference> opened = new ArrayList<>();
    private final Set<Exception> openTraces = Collections.newSetFromMap(new IdentityHashMap<>());

    @After
    public void checkAllClosed() throws Exception {
        // Decrement one time to simulate closing the netty buffer after the stream is captured
        for (ReleasableBytesReference r : opened) {
            r.decRef();
        }
        // Now that we've decremented, we expect all streams will have been closed
        Iterator<Exception> iter = openTraces.iterator();
        if (iter.hasNext()) {
            throw new Exception("didn't close iterator - cause is opening location", iter.next());
        }
        for (ReleasableBytesReference r : opened) {
            assertThat(r.hasReferences(), equalTo(false));
        }
    }

    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        // Grab an exception at the opening location, so we can throw it if we don't close
        Exception trace = new Exception();
        openTraces.add(trace);

        ReleasableBytesReference counted = new ReleasableBytesReference(bytesReference, () -> openTraces.remove(trace));

        /*
         * Grab a reference to the bytes ref we're using, so we can close it after the
         * test to simulate the underlying netter butter closing after the test.
         */
        opened.add(counted);
        return counted.streamInput();
    }

    public void testBigIntArrayLivesAfterReleasableIsDecremented() throws IOException {
        IntArray testData = BigArrays.NON_RECYCLING_INSTANCE.newIntArray(1, false);
        testData.set(0, 1);

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        ReleasableBytesReference ref = ReleasableBytesReference.wrap(out.bytes());

        try (IntArray in = IntArray.readFrom(ref.streamInput())) {
            ref.decRef();
            assertThat(ref.hasReferences(), equalTo(true));

            assertThat(in.size(), equalTo(testData.size()));
            assertThat(in.get(0), equalTo(1));
        }
        assertThat(ref.hasReferences(), equalTo(false));
    }

    public void testBigDoubleArrayLivesAfterReleasableIsDecremented() throws IOException {
        DoubleArray testData = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(1, false);
        testData.set(0, 1);

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        ReleasableBytesReference ref = ReleasableBytesReference.wrap(out.bytes());

        try (DoubleArray in = DoubleArray.readFrom(ref.streamInput())) {
            ref.decRef();
            assertThat(ref.hasReferences(), equalTo(true));

            assertThat(in.size(), equalTo(testData.size()));
            assertThat(in.get(0), equalTo(1.0));
        }
        assertThat(ref.hasReferences(), equalTo(false));
    }

    public void testBigLongArrayLivesAfterReleasableIsDecremented() throws IOException {
        LongArray testData = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(1, false);
        testData.set(0, 1);

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        ReleasableBytesReference ref = ReleasableBytesReference.wrap(out.bytes());

        try (LongArray in = LongArray.readFrom(ref.streamInput())) {
            ref.decRef();
            assertThat(ref.hasReferences(), equalTo(true));

            assertThat(in.size(), equalTo(testData.size()));
            assertThat(in.get(0), equalTo(1L));
        }
        assertThat(ref.hasReferences(), equalTo(false));
    }

    public void testBigByteArrayLivesAfterReleasableIsDecremented() throws IOException {
        ByteArray testData = BigArrays.NON_RECYCLING_INSTANCE.newByteArray(1, false);
        testData.set(0L, (byte) 1);

        BytesStreamOutput out = new BytesStreamOutput();
        testData.writeTo(out);

        ReleasableBytesReference ref = ReleasableBytesReference.wrap(out.bytes());

        try (ByteArray in = ByteArray.readFrom(ref.streamInput())) {
            ref.decRef();
            assertThat(ref.hasReferences(), equalTo(true));

            assertThat(in.size(), equalTo(testData.size()));
            assertThat(in.get(0), equalTo((byte) 1));
        }
        assertThat(ref.hasReferences(), equalTo(false));
    }

}
