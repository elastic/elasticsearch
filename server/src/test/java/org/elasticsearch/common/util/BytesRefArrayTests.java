/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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
                (out, value) -> value.writeTo(out),
                in -> new BytesRefArray(in, mockBigArrays()),
                Version.CURRENT
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
