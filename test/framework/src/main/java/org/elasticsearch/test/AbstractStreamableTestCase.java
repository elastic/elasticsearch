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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

import java.io.IOException;
import java.util.Collections;

public abstract class AbstractStreamableTestCase<T extends Streamable> extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    /**
     * Creates an empty instance to use when deserialising the
     * {@link Streamable}. This usually returns an instance created using the
     * zer-arg constructor
     */
    protected abstract T createBlankInstance();

    /**
     * Returns a {@link CopyFunction} that can be used to make an exact copy of
     * the given instance. This defaults to a function that uses
     * {@link #copyInstance(Streamable, Version)} to create the copy.
     */
    protected CopyFunction<T> getCopyFunction() {
        return (original) -> copyInstance(original, Version.CURRENT);
    }

    /**
     * Returns a {@link MutateFunction} that can be used to make create a copy
     * of the given instance that is different to this instance. This defaults
     * to null.
     */
    // TODO: Make this abstract when all sub-classes implement this (https://github.com/elastic/elasticsearch/issues/25929)
    protected MutateFunction<T> getMutateFunction() {
        return null;
    }

    /**
     * Tests that the equals and hashcode methods are consistent and copied
     * versions of the instance have are equal.
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), getCopyFunction(), getMutateFunction());
        }
    }

    /**
     * Test serialization and deserialization of the test instance.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    /**
     * Serialize the given instance and asserts that both are equal
     */
    protected T assertSerialization(T testInstance) throws IOException {
        T deserializedInstance = copyInstance(testInstance, Version.CURRENT);
        assertEquals(testInstance, deserializedInstance);
        assertEquals(testInstance.hashCode(), deserializedInstance.hashCode());
        assertNotSame(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    /**
     * Round trip {@code instance} through binary serialization, setting the wire compatibility version to {@code version}.
     */
    private T copyInstance(T instance, Version version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            instance.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(),
                    getNamedWriteableRegistry())) {
                in.setVersion(version);
                T newInstance = createBlankInstance();
                newInstance.readFrom(in);
                return newInstance;
            }
        }
    }

    /**
     * Get the {@link NamedWriteableRegistry} to use when de-serializing the object.
     * 
     * Override this method if you need to register {@link NamedWriteable}s for the test object to de-serialize.
     * 
     * By default this will return a {@link NamedWriteableRegistry} with no registered {@link NamedWriteable}s
     */
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.emptyList());
    }

}
