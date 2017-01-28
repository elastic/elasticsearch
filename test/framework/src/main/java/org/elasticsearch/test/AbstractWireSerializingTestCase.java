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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.io.stream.Writeable.Reader;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractWireSerializingTestCase<T extends Writeable> extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    /**
     * Returns a {@link Reader} that can be used to de-serialize the instance
     */
    protected abstract Reader<T> instanceReader();

    /**
     * Tests that the equals and hashcode methods are consistent and copied
     * versions of the instance have are equal.
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T firstInstance = createTestInstance();
            assertFalse("instance is equal to null", firstInstance.equals(null));
            assertFalse("instance is equal to incompatible type", firstInstance.equals(""));
            assertEquals("instance is not equal to self", firstInstance, firstInstance);
            assertThat("same instance's hashcode returns different values if called multiple times", firstInstance.hashCode(),
                    equalTo(firstInstance.hashCode()));

            T secondInstance = copyInstance(firstInstance);
            assertEquals("instance is not equal to self", secondInstance, secondInstance);
            if (false == firstInstance.equals(secondInstance)) {
                firstInstance.equals(secondInstance);
            assertEquals("instance is not equal to its copy", firstInstance, secondInstance);
            }
            assertEquals("equals is not symmetric", secondInstance, firstInstance);
            assertThat("instance copy's hashcode is different from original hashcode", secondInstance.hashCode(),
                    equalTo(firstInstance.hashCode()));

            T thirdInstance = copyInstance(secondInstance);
            assertEquals("instance is not equal to self", thirdInstance, thirdInstance);
            assertEquals("instance is not equal to its copy", secondInstance, thirdInstance);
            assertThat("instance copy's hashcode is different from original hashcode", secondInstance.hashCode(),
                    equalTo(thirdInstance.hashCode()));
            assertEquals("equals is not transitive", firstInstance, thirdInstance);
            assertThat("instance copy's hashcode is different from original hashcode", firstInstance.hashCode(),
                    equalTo(thirdInstance.hashCode()));
            assertEquals("equals is not symmetric", thirdInstance, secondInstance);
            assertEquals("equals is not symmetric", thirdInstance, firstInstance);
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
        T deserializedInstance = copyInstance(testInstance);
        assertEquals(testInstance, deserializedInstance);
        assertEquals(testInstance.hashCode(), deserializedInstance.hashCode());
        assertNotSame(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    private T copyInstance(T instance) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            instance.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(),
                    getNamedWriteableRegistry())) {
                return instanceReader().read(in);
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
