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
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;

/**
 * Standard test case for testing wire serialization. If the class being tested
 * extends {@link Writeable} then prefer extending {@link AbstractWireSerializingTestCase}.
 */
public abstract class AbstractWireTestCase<T> extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    /**
     * Returns an instance which is mutated slightly so it should not be equal
     * to the given instance.
     */
    // TODO: Make this abstract when all sub-classes implement this (https://github.com/elastic/elasticsearch/issues/25929)
    protected T mutateInstance(T instance) throws IOException {
        return null;
    }

    /**
     * Tests that the equals and hashcode methods are consistent and copied
     * versions of the instance are equal.
     */
    public final void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copyInstance, this::mutateInstance);
        }
    }

    /**
     * Test serialization and deserialization of the test instance.
     */
    public final void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    /**
     * Serialize the given instance and asserts that both are equal.
     */
    protected final void assertSerialization(T testInstance) throws IOException {
        assertSerialization(testInstance, Version.CURRENT);
    }

    /**
     * Assert that instances copied at a particular version are equal. The version is useful
     * for sanity checking the backwards compatibility of the wire. It isn't a substitute for
     * real backwards compatibility tests but it is *so* much faster.
     */
    protected final void assertSerialization(T testInstance, Version version) throws IOException {
        T deserializedInstance = copyInstance(testInstance, version);
        assertEqualInstances(testInstance, deserializedInstance);
    }

    /**
     * Assert that two instances are equal. This is intentionally not final so we can override
     * how equality is checked.
     */
    protected void assertEqualInstances(T expectedInstance, T newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    protected final T copyInstance(T instance) throws IOException {
        return copyInstance(instance, Version.CURRENT);
    }

    /**
     * Copy the instance as by reading and writing using the code specific to the provided version.
     * The version is useful for sanity checking the backwards compatibility of the wire. It isn't
     * a substitute for real backwards compatibility tests but it is *so* much faster.
     */
    protected abstract T copyInstance(T instance, Version version) throws IOException;

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
