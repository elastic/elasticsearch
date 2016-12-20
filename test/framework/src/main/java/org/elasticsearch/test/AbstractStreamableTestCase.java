/*
 * ELASTICSEARCH CONFIDENTIAL
 *
 * Copyright (c) 2016 Elasticsearch BV. All Rights Reserved.
 *
 * Notice: this software, and all information contained
 * therein, is the exclusive property of Elasticsearch BV
 * and its licensors, if any, and is protected under applicable
 * domestic and foreign law, and international treaties.
 *
 * Reproduction, republication or distribution without the
 * express written consent of Elasticsearch BV is
 * strictly prohibited.
 */
package org.elasticsearch.test;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractStreamableTestCase<T extends Streamable> extends ESTestCase {
    protected static final int NUMBER_OF_TESTQUERIES = 20;

    protected abstract T createTestInstance();

    protected abstract T createBlankInstance();

    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            T firstInstance = createTestInstance();
            assertFalse("query is equal to null", firstInstance.equals(null));
            assertFalse("query is equal to incompatible type", firstInstance.equals(""));
            assertTrue("query is not equal to self", firstInstance.equals(firstInstance));
            assertThat("same query's hashcode returns different values if called multiple times", firstInstance.hashCode(),
                    equalTo(firstInstance.hashCode()));

            T secondInstance = copyInstance(firstInstance);
            assertTrue("query is not equal to self", secondInstance.equals(secondInstance));
            assertTrue("query is not equal to its copy", firstInstance.equals(secondInstance));
            assertTrue("equals is not symmetric", secondInstance.equals(firstInstance));
            assertThat("query copy's hashcode is different from original hashcode", secondInstance.hashCode(),
                    equalTo(firstInstance.hashCode()));

            T thirdInstance = copyInstance(secondInstance);
            assertTrue("query is not equal to self", thirdInstance.equals(thirdInstance));
            assertTrue("query is not equal to its copy", secondInstance.equals(thirdInstance));
            assertThat("query copy's hashcode is different from original hashcode", secondInstance.hashCode(),
                    equalTo(thirdInstance.hashCode()));
            assertTrue("equals is not transitive", firstInstance.equals(thirdInstance));
            assertThat("query copy's hashcode is different from original hashcode", firstInstance.hashCode(),
                    equalTo(thirdInstance.hashCode()));
            assertTrue("equals is not symmetric", thirdInstance.equals(secondInstance));
            assertTrue("equals is not symmetric", thirdInstance.equals(firstInstance));
        }
    }

    /**
     * Test serialization and deserialization of the test query.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTQUERIES; runs++) {
            T testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    /**
     * Serialize the given query builder and asserts that both are equal
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
                    new NamedWriteableRegistry(Collections.emptyList()))) {
                T newInstance = createBlankInstance();
                newInstance.readFrom(in);
                return newInstance;
            }
        }
    }

}
