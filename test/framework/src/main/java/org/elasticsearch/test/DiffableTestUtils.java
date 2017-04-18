/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.io.stream.Writeable.Reader;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.AbstractWireSerializingTestCase.NUMBER_OF_TEST_RUNS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Utilities that simplify testing of diffable classes
 */
public final class DiffableTestUtils {
    protected static final int NUMBER_OF_DIFF_TEST_RUNS = NUMBER_OF_TEST_RUNS;

    private DiffableTestUtils() {

    }

    /**
     * Asserts that changes are applied correctly, i.e. that applying diffs to localInstance produces that object
     * equal but not the same as the remoteChanges instance.
     */
    public static <T extends Diffable<T>> T assertDiffApplication(T remoteChanges, T localInstance, Diff<T> diffs) {
        T localChanges = diffs.apply(localInstance);
        assertEquals(remoteChanges, localChanges);
        assertEquals(remoteChanges.hashCode(), localChanges.hashCode());
        assertNotSame(remoteChanges, localChanges);
        return localChanges;
    }

    /**
     * Simulates sending diffs over the wire
     */
    public static <T extends Writeable> T copyInstance(T diffs, NamedWriteableRegistry namedWriteableRegistry,
                                                                  Reader<T> reader) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            diffs.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return reader.read(in);
            }
        }
    }

    /**
     * Tests making random changes to an object, calculating diffs for these changes, sending this
     * diffs over the wire and appling these diffs on the other side.
     */
    public static <T extends Diffable<T>> void testDiffableSerialization(Supplier<T> testInstance,
                                                                         Function<T, T> modifier,
                                                                         NamedWriteableRegistry namedWriteableRegistry,
                                                                         Reader<T> reader,
                                                                         Reader<Diff<T>> diffReader) throws IOException {
        T remoteInstance = testInstance.get();
        T localInstance = assertSerialization(remoteInstance, namedWriteableRegistry, reader);
        for (int runs = 0; runs < NUMBER_OF_DIFF_TEST_RUNS; runs++) {
            T remoteChanges = modifier.apply(remoteInstance);
            Diff<T> remoteDiffs = remoteChanges.diff(remoteInstance);
            Diff<T> localDiffs = copyInstance(remoteDiffs, namedWriteableRegistry, diffReader);
            localInstance = assertDiffApplication(remoteChanges, localInstance, localDiffs);
            remoteInstance = remoteChanges;
        }
    }

    /**
     * Asserts that testInstance can be correctly.
     */
    public static  <T extends Writeable> T assertSerialization(T testInstance, NamedWriteableRegistry namedWriteableRegistry,
                                                          Reader<T> reader) throws IOException {
        T deserializedInstance = copyInstance(testInstance, namedWriteableRegistry, reader);
        assertEquals(testInstance, deserializedInstance);
        assertEquals(testInstance.hashCode(), deserializedInstance.hashCode());
        assertNotSame(testInstance, deserializedInstance);
        return deserializedInstance;
    }

}
