/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    public static <T extends Writeable> T copyInstance(T diffs, NamedWriteableRegistry namedWriteableRegistry, Reader<T> reader)
        throws IOException {
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
    public static <T extends Diffable<T>> void testDiffableSerialization(
        Supplier<T> testInstance,
        Function<T, T> modifier,
        NamedWriteableRegistry namedWriteableRegistry,
        Reader<T> reader,
        Reader<Diff<T>> diffReader
    ) throws IOException {
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
    public static <T extends Writeable> T assertSerialization(
        T testInstance,
        NamedWriteableRegistry namedWriteableRegistry,
        Reader<T> reader
    ) throws IOException {
        T deserializedInstance = copyInstance(testInstance, namedWriteableRegistry, reader);
        assertEquals(testInstance, deserializedInstance);
        assertEquals(testInstance.hashCode(), deserializedInstance.hashCode());
        assertNotSame(testInstance, deserializedInstance);
        return deserializedInstance;
    }

}
