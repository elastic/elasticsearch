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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SerializableBytesRefHashTests extends AbstractBytesRefHashTests {

    @Override
    protected SerializableBytesRefHash createTestInstance() {
        final float maxLoadFactor = 0.6f + randomFloat() * 0.39f;
        return new SerializableBytesRefHash(randomIntBetween(0, 100), maxLoadFactor, mockBigArrays());
    }

    // assert that the copied hash table has the same entries
    private void assertEquality(int size, AbstractBytesRefHash original, AbstractBytesRefHash copy) {
        BytesRef scratch = new BytesRef();
        int actual = 0;

        assertEquals(original.capacity(), copy.capacity());

        for (int i = 0; i < original.capacity(); ++i) {
            long id = original.id(i);
            long idOfCopy = copy.id(i);
            assertEquals(id, idOfCopy);

            if (id != -1) {
                ++actual;
                copy.get(id, scratch);
                assertEquals(id, original.find(scratch));
                assertEquals(id, copy.find(scratch));
            }
        }
        assertEquals(size, actual);
    }

    // technical debt: ideally we would implement {@link AbstractWireSerializingTestCase},
    // however that requires changes all over the place, that's why this is kept simple
    public void testSerialization() throws IOException {
        SerializableBytesRefHash hash = createTestInstance();
        BytesRefBuilder ref = new BytesRefBuilder();
        int runs = randomIntBetween(2, 20);

        for (int j = 0; j < runs; j++) {
            Set<String> strings = new HashSet<>();
            int entries = randomIntBetween(100, 5000);

            for (int i = 0; i < entries; i++) {
                String str = randomUnicodeOfLengthBetween(4, 20);
                ref.copyChars(str);
                hash.add(ref.get());
                strings.add(str);
            }

            SerializableBytesRefHash copy = copyInstance(
                hash,
                writableRegistry(),
                (out, value) -> value.writeTo(out),
                in -> SerializableBytesRefHash.loadFromStream(in, mockBigArrays()),
                Version.CURRENT
            );

            assertEquality(strings.size(), hash, copy);
            assertAllIn(strings, copy);
            hash.close();
            copy.close();
            hash = createTestInstance();
        }
        hash.close();
    }

}
