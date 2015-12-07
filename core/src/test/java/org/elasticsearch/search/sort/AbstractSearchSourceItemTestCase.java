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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

//TODO maybe merge with AbstractShapeBuilderTestCase once #14933 is in?
public abstract class AbstractSearchSourceItemTestCase<T extends NamedWriteable<T> & ToXContent & ParameterParser<T>> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private NamedWriteableRegistry namedWriteableRegistry;

    public class RegistryItem {
        public Class<T> type;
        public T prototype;

        public RegistryItem(Class<T> type, T prototype) {
            this.type = type;
            this.prototype = prototype;
        }
    }

    @Before
    public void setup() {
        namedWriteableRegistry = new NamedWriteableRegistry();
        RegistryItem item = getPrototype();
        namedWriteableRegistry.registerPrototype(item.type, item.prototype);
    }

    @After
    public void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /** Returns the prototype of the named writable under test. */
    protected abstract RegistryItem getPrototype();

    /** Returns random shape that is put under test */
    protected abstract T createTestItem();

    /** Returns mutated version of original so the returned shape is different in terms of equals/hashcode */
    protected abstract T mutate(T original) throws IOException;

    protected abstract ParameterParser<T> getItemParser();

    /**
     * Test that creates new shape from a random test shape and checks both for equality
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T testItem = createTestItem();
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                contentBuilder.prettyPrint();
            }
            XContentBuilder builder = testItem.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
            XContentParser itemParser = XContentHelper.createParser(builder.bytes());
            XContentHelper.createParser(builder.bytes());
            itemParser.nextToken();
            NamedWriteable<T> parsedItem= getItemParser().fromXContent(itemParser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    /**
     * Test serialization and deserialization of the test shape.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T testShape = createTestItem();
            T deserializedShape = copyItem(testShape);
            assertEquals(deserializedShape, testShape);
            assertEquals(deserializedShape.hashCode(), testShape.hashCode());
            assertNotSame(deserializedShape, testShape);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            T firstShape = createTestItem();
            assertFalse("shape is equal to null", firstShape.equals(null));
            assertFalse("shape is equal to incompatible type", firstShape.equals(""));
            assertTrue("shape is not equal to self", firstShape.equals(firstShape));
            assertThat("same shape's hashcode returns different values if called multiple times", firstShape.hashCode(),
                    equalTo(firstShape.hashCode()));
            assertThat("different shapes should not be equal", mutate(firstShape), not(equalTo(firstShape)));
            assertThat("different shapes should have different hashcode", mutate(firstShape).hashCode(), not(equalTo(firstShape.hashCode())));

            T secondShape = copyItem(firstShape);
            assertTrue("shape is not equal to self", secondShape.equals(secondShape));
            assertTrue("shape is not equal to its copy", firstShape.equals(secondShape));
            assertTrue("equals is not symmetric", secondShape.equals(firstShape));
            assertThat("shape copy's hashcode is different from original hashcode", secondShape.hashCode(), equalTo(firstShape.hashCode()));

            T thirdShape = copyItem(secondShape);
            assertTrue("shape is not equal to self", thirdShape.equals(thirdShape));
            assertTrue("shape is not equal to its copy", secondShape.equals(thirdShape));
            assertThat("shape copy's hashcode is different from original hashcode", secondShape.hashCode(), equalTo(thirdShape.hashCode()));
            assertTrue("equals is not transitive", firstShape.equals(thirdShape));
            assertThat("shape copy's hashcode is different from original hashcode", firstShape.hashCode(), equalTo(thirdShape.hashCode()));
            assertTrue("equals is not symmetric", thirdShape.equals(secondShape));
            assertTrue("equals is not symmetric", thirdShape.equals(firstShape));
        }
    }

    protected T copyItem(T original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                @SuppressWarnings("unchecked")
                T prototype = (T) namedWriteableRegistry.getPrototype(getPrototype().type, original.getWriteableName());
                T copy = (T) prototype.readFrom(in);
                return copy;
            }
        }
    }
}
