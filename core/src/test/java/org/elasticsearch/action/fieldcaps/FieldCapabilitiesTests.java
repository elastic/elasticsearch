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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class FieldCapabilitiesTests extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    public void testBuilder() {
        FieldCapabilities.Builder builder =
            new FieldCapabilities.Builder("field", "type");
        builder.add("index1", true, false);
        builder.add("index2", true, false);
        builder.add("index3", true, false);

        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(true));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertNull(cap1.indices());
            assertNull(cap1.nonSearchableIndices());
            assertNull(cap1.nonAggregatableIndices());

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(true));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[]{"index1", "index2", "index3"}));
            assertNull(cap2.nonSearchableIndices());
            assertNull(cap2.nonAggregatableIndices());
        }

        builder = new FieldCapabilities.Builder("field", "type");
        builder.add("index1", false, true);
        builder.add("index2", true, false);
        builder.add("index3", false, false);
        {
            FieldCapabilities cap1 = builder.build(false);
            assertThat(cap1.isSearchable(), equalTo(false));
            assertThat(cap1.isAggregatable(), equalTo(false));
            assertNull(cap1.indices());
            assertThat(cap1.nonSearchableIndices(), equalTo(new String[]{"index1", "index3"}));
            assertThat(cap1.nonAggregatableIndices(), equalTo(new String[]{"index2", "index3"}));

            FieldCapabilities cap2 = builder.build(true);
            assertThat(cap2.isSearchable(), equalTo(false));
            assertThat(cap2.isAggregatable(), equalTo(false));
            assertThat(cap2.indices().length, equalTo(3));
            assertThat(cap2.indices(), equalTo(new String[]{"index1", "index2", "index3"}));
            assertThat(cap1.nonSearchableIndices(), equalTo(new String[]{"index1", "index3"}));
            assertThat(cap1.nonAggregatableIndices(), equalTo(new String[]{"index2", "index3"}));
        }
    }

    /**
     * Tests that the equals and hashcode methods are consistent and copied
     * versions of the instance have are equal.
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            FieldCapabilities firstInstance = randomFieldCaps();
            assertFalse("instance is equal to null",
                firstInstance.equals(null));
            assertFalse("instance is equal to incompatible type",
                firstInstance.equals(""));
            assertEquals("instance is not equal to self",
                firstInstance, firstInstance);
            assertThat("same instance's hashcode returns different values " +
                    "if called multiple times",
                firstInstance.hashCode(),
                equalTo(firstInstance.hashCode()));

            FieldCapabilities secondInstance = copyInstance(firstInstance);
            assertEquals("instance is not equal to self",
                secondInstance, secondInstance);
            assertEquals("instance is not equal to its copy",
                firstInstance, secondInstance);
            assertEquals("equals is not symmetric",
                secondInstance, firstInstance);
            assertThat("instance copy's hashcode is different from original hashcode",
                secondInstance.hashCode(),
                equalTo(firstInstance.hashCode()));

            FieldCapabilities thirdInstance = copyInstance(secondInstance);
            assertEquals("instance is not equal to self",
                thirdInstance, thirdInstance);
            assertEquals("instance is not equal to its copy",
                secondInstance, thirdInstance);
            assertThat("instance copy's hashcode is different from original hashcode",
                secondInstance.hashCode(),
                equalTo(thirdInstance.hashCode()));
            assertEquals("equals is not transitive",
                firstInstance, thirdInstance);
            assertThat("instance copy's hashcode is different from original hashcode",
                firstInstance.hashCode(),
                equalTo(thirdInstance.hashCode()));
            assertEquals("equals is not symmetric",
                thirdInstance, secondInstance);
            assertEquals("equals is not symmetric",
                thirdInstance, firstInstance);
        }
    }

    /**
     * Test serialization and deserialization of the test instance.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            FieldCapabilities testInstance = randomFieldCaps();
            assertSerialization(testInstance);
        }
    }

    /**
     * Serialize the given instance and asserts that both are equal
     */
    protected FieldCapabilities assertSerialization(FieldCapabilities testInstance)
        throws IOException {
        FieldCapabilities deserializedInstance = copyInstance(testInstance);
        assertEquals(testInstance, deserializedInstance);
        assertEquals(testInstance.hashCode(), deserializedInstance.hashCode());
        assertNotSame(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    private FieldCapabilities copyInstance(FieldCapabilities instance) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            instance.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(),
                getNamedWriteableRegistry())) {
                return new FieldCapabilities(in);
            }
        }
    }

    /**
     * Get the {@link NamedWriteableRegistry} to use when de-serializing the object.
     *
     * Override this method if you need to register {@link NamedWriteable}s for the
     * test object to de-serialize.
     *
     * By default this will return a {@link NamedWriteableRegistry} with no
     * registered {@link NamedWriteable}s
     */
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.emptyList());
    }

    static FieldCapabilities randomFieldCaps() {
        String[] indices = null;
        if (randomBoolean()) {
            indices = new String[randomIntBetween(1, 5)];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonSearchableIndices = null;
        if (randomBoolean()) {
            nonSearchableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonSearchableIndices.length; i++) {
                nonSearchableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        String[] nonAggregatableIndices = null;
        if (randomBoolean()) {
            nonAggregatableIndices = new String[randomIntBetween(0, 5)];
            for (int i = 0; i < nonAggregatableIndices.length; i++) {
                nonAggregatableIndices[i] = randomAlphaOfLengthBetween(5, 20);
            }
        }
        return new FieldCapabilities(randomAlphaOfLengthBetween(5, 20),
            randomAlphaOfLengthBetween(5, 20), randomBoolean(), randomBoolean(),
            indices, nonSearchableIndices, nonAggregatableIndices);
    }
}
