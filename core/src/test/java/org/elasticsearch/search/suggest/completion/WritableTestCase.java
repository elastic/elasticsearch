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

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Base class for testing serialization and equality for
 * {@link Writeable} models
 */
public abstract class WritableTestCase<M extends Writeable> extends ESTestCase {

    protected static final int NUMBER_OF_RUNS = 20;

    /**
     * create random model that is put under test
     */
    protected abstract M createTestModel();

    /**
     * mutate the given model so the returned model is different
     */
    protected abstract M createMutation(M original) throws IOException;

    /**
     * Read from a stream.
     */
    protected abstract M readFrom(StreamInput in) throws IOException;

    /**
     * Test serialization and deserialization of the tested model.
     */
    public void testSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            M testModel = createTestModel();
            M deserializedModel = copyModel(testModel);
            assertEquals(testModel, deserializedModel);
            assertEquals(testModel.hashCode(), deserializedModel.hashCode());
            assertNotSame(testModel, deserializedModel);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    @SuppressWarnings("unchecked")
    public void testEqualsAndHashcode() throws IOException {
        M firstModel = createTestModel();
        String modelName = firstModel.getClass().getSimpleName();
        assertFalse(modelName + " is equal to null", firstModel.equals(null));
        assertFalse(modelName + " is equal to incompatible type", firstModel.equals(""));
        assertTrue(modelName + " is not equal to self", firstModel.equals(firstModel));
        assertThat("same "+ modelName + "'s hashcode returns different values if called multiple times", firstModel.hashCode(),
                equalTo(firstModel.hashCode()));
        assertThat("different " + modelName + " should not be equal", createMutation(firstModel), not(equalTo(firstModel)));

        M secondModel = copyModel(firstModel);
        assertTrue(modelName + " is not equal to self", secondModel.equals(secondModel));
        assertTrue(modelName + " is not equal to its copy", firstModel.equals(secondModel));
        assertTrue("equals is not symmetric", secondModel.equals(firstModel));
        assertThat(modelName + " copy's hashcode is different from original hashcode", secondModel.hashCode(),
                equalTo(firstModel.hashCode()));

        M thirdModel = copyModel(secondModel);
        assertTrue(modelName + " is not equal to self", thirdModel.equals(thirdModel));
        assertTrue(modelName + " is not equal to its copy", secondModel.equals(thirdModel));
        assertThat(modelName + " copy's hashcode is different from original hashcode", secondModel.hashCode(),
                equalTo(thirdModel.hashCode()));
        assertTrue("equals is not transitive", firstModel.equals(thirdModel));
        assertThat(modelName + " copy's hashcode is different from original hashcode", firstModel.hashCode(),
                equalTo(thirdModel.hashCode()));
        assertTrue(modelName + " equals is not symmetric", thirdModel.equals(secondModel));
        assertTrue(modelName + " equals is not symmetric", thirdModel.equals(firstModel));
    }

    private M copyModel(M original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), provideNamedWritableRegistry())) {
                return readFrom(in);
            }
        }
    }

    protected NamedWriteableRegistry provideNamedWritableRegistry() {
        return new NamedWriteableRegistry();
    }
}
