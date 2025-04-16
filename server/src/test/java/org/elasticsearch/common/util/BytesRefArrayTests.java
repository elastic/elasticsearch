/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class BytesRefArrayTests extends ESTestCase {

    public static BytesRefArray randomArray() {
        return randomArray(randomIntBetween(0, 100), randomIntBetween(10, 50), mockBigArrays());
    }

    public static BytesRefArray randomArray(long capacity, int entries, BigArrays bigArrays) {
        BytesRefArray bytesRefs = new BytesRefArray(capacity, bigArrays);
        BytesRefBuilder ref = new BytesRefBuilder();

        for (int i = 0; i < entries; i++) {
            String str = randomUnicodeOfLengthBetween(4, 20);
            ref.copyChars(str);
            bytesRefs.append(ref.get());
        }

        return bytesRefs;
    }

    public void testRandomWithSerialization() throws IOException {
        int runs = randomIntBetween(2, 20);
        BytesRefArray array = randomArray();

        for (int j = 0; j < runs; j++) {
            BytesRefArray copy = copyInstance(
                array,
                writableRegistry(),
                StreamOutput::writeWriteable,
                in -> new BytesRefArray(in, mockBigArrays()),
                TransportVersion.current()
            );

            assertEquality(array, copy);
            copy.close();
            array.close();
            array = randomArray();
        }
        array.close();
    }

    public void testTakeOwnership() {
        BytesRefArray array = randomArray();
        long size = array.size();
        BytesRefArray newOwnerOfArray = BytesRefArray.takeOwnershipOf(array);

        assertNotEquals(array, newOwnerOfArray);
        assertEquals(0, array.size());
        assertEquals(size, newOwnerOfArray.size());

        array.close();
        newOwnerOfArray.close();
    }

    public void testLookup() throws IOException {
        int size = randomIntBetween(0, 16 * 1024);
        BytesRefArray array = new BytesRefArray(randomIntBetween(0, size), mockBigArrays());
        try {
            BytesRef[] values = new BytesRef[size];
            for (int i = 0; i < size; i++) {
                BytesRef bytesRef = new BytesRef(randomByteArrayOfLength(between(1, 20)));
                if (bytesRef.length > 0 && randomBoolean()) {
                    bytesRef.offset = randomIntBetween(0, bytesRef.length - 1);
                    bytesRef.length = randomIntBetween(0, bytesRef.length - bytesRef.offset);
                }
                values[i] = bytesRef;
                if (randomBoolean()) {
                    bytesRef = BytesRef.deepCopyOf(bytesRef);
                }
                array.append(bytesRef);
            }
            int copies = randomIntBetween(0, 3);
            for (int i = 0; i < copies; i++) {
                BytesRefArray inArray = array;
                array = copyInstance(
                    inArray,
                    writableRegistry(),
                    StreamOutput::writeWriteable,
                    in -> new BytesRefArray(in, mockBigArrays()),
                    TransportVersion.current()
                );
                assertEquality(inArray, array);
                inArray.close();
            }
            assertThat(array.size(), equalTo((long) size));
            BytesRef bytes = new BytesRef();
            for (int i = 0; i < size; i++) {
                int pos = randomIntBetween(0, size - 1);
                bytes = array.get(pos, bytes);
                assertThat(bytes, equalTo(values[pos]));
            }
        } finally {
            array.close();
        }
    }

    public void testReadWritten() {
        testReadWritten(false);
    }

    public void testReadWrittenHalfEmpty() {
        testReadWritten(true);
    }

    private void testReadWritten(boolean halfEmpty) {
        List<BytesRef> values = new ArrayList<>();
        int bytes = PageCacheRecycler.PAGE_SIZE_IN_BYTES * between(2, 20);
        int used = 0;
        while (used < bytes) {
            String str = halfEmpty && randomBoolean() ? "" : randomAlphaOfLengthBetween(0, 200);
            BytesRef v = new BytesRef(str);
            used += v.length;
            values.add(v);
        }
        testReadWritten(values, randomBoolean() ? bytes : between(0, bytes));
    }

    public void testReadWrittenRepeated() {
        testReadWrittenRepeated(false, between(2, 3000));
    }

    public void testReadWrittenRepeatedPowerOfTwo() {
        testReadWrittenRepeated(false, 1024);
    }

    public void testReadWrittenRepeatedHalfEmpty() {
        testReadWrittenRepeated(true, between(1, 3000));
    }

    public void testReadWrittenRepeatedHalfEmptyPowerOfTwo() {
        testReadWrittenRepeated(true, 1024);
    }

    public void testReadWrittenRepeated(boolean halfEmpty, int listSize) {
        List<BytesRef> values = randomList(2, 10, () -> {
            String str = halfEmpty && randomBoolean() ? "" : randomAlphaOfLengthBetween(0, 10);
            return new BytesRef(str);
        });
        testReadWritten(IntStream.range(0, listSize).mapToObj(i -> values).flatMap(List::stream).toList(), 10);
    }

    private void testReadWritten(List<BytesRef> values, int initialCapacity) {
        try (BytesRefArray array = new BytesRefArray(initialCapacity, mockBigArrays())) {
            for (BytesRef v : values) {
                array.append(v);
            }
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < values.size(); i++) {
                array.get(i, scratch);
                assertThat(scratch, equalTo(values.get(i)));
            }
        }
    }

    private static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private void assertEquality(BytesRefArray original, BytesRefArray copy) {
        BytesRef scratch = new BytesRef();
        BytesRef scratch2 = new BytesRef();

        assertEquals(original.size(), copy.size());

        // check that all keys of original can be found in the copy
        for (int i = 0; i < original.size(); ++i) {
            original.get(i, scratch);
            copy.get(i, scratch2);
            assertEquals(scratch, scratch2);
        }
    }
}
