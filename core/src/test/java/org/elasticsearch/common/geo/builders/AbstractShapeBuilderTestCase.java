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

package org.elasticsearch.common.geo.builders;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.hamcrest.Matchers.*;

public abstract class AbstractShapeBuilderTestCase<SB extends ShapeBuilder> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 1;
    private static final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry();

    /**
     * Setup for the whole base test class.
     */
    @BeforeClass
    public static void init() {
        namedWriteableRegistry.registerPrototype(ShapeBuilder.class, PointBuilder.PROTOTYPE);
    }

    /**
     * Create the shape that under test
     */
    protected abstract SB createTestShapeBuilder();

    /**
     * mutate the given query so the returned query is different
     */
    protected abstract SB mutate(SB original);

    /**
     * Generic test that creates new shape from a random test shape and checks both for equality
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB testShape = createTestShapeBuilder();
            XContentBuilder builder = toXContent(testShape, randomFrom(XContentType.values()));

            XContentParser shapeParser = XContentHelper.createParser(builder.bytes());
            shapeParser.nextToken();
            ShapeBuilder parsedShape = ShapeBuilder.parse(shapeParser);
            assertNotSame(testShape, parsedShape);
            assertEquals(testShape, parsedShape);
            assertEquals(testShape.hashCode(), parsedShape.hashCode());
        }
    }

    protected static XContentBuilder toXContent(ShapeBuilder shape, XContentType contentType) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        return shape.toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    /**
     * Test serialization and deserialization of the test shape.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB testShape = createTestShapeBuilder();
            SB deserializedShape = copyShape(testShape);
            assertEquals(deserializedShape, testShape);
            assertEquals(deserializedShape.hashCode(), testShape.hashCode());
            assertNotSame(deserializedShape, testShape);
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB firstShape = createTestShapeBuilder();
            assertFalse("shape is equal to null", firstShape.equals(null));
            assertFalse("shape is equal to incompatible type", firstShape.equals(""));
            assertTrue("shape is not equal to self", firstShape.equals(firstShape));
            assertThat("same shape's hashcode returns different values if called multiple times", firstShape.hashCode(),
                    equalTo(firstShape.hashCode()));
            assertThat("different shapes should not be equal", mutate(firstShape), not(equalTo(firstShape)));
            assertThat("different shapes should have different hashcode", mutate(firstShape).hashCode(), not(equalTo(firstShape.hashCode())));

            SB secondShape = copyShape(firstShape);
            assertTrue("shape is not equal to self", secondShape.equals(secondShape));
            assertTrue("shape is not equal to its copy", firstShape.equals(secondShape));
            assertTrue("equals is not symmetric", secondShape.equals(firstShape));
            assertThat("shape copy's hashcode is different from original hashcode", secondShape.hashCode(), equalTo(firstShape.hashCode()));

            SB thirdShape = copyShape(secondShape);
            assertTrue("shape is not equal to self", thirdShape.equals(thirdShape));
            assertTrue("shape is not equal to its copy", secondShape.equals(thirdShape));
            assertThat("shape copy's hashcode is different from original hashcode", secondShape.hashCode(), equalTo(thirdShape.hashCode()));
            assertTrue("equals is not transitive", firstShape.equals(thirdShape));
            assertThat("shape copy's hashcode is different from original hashcode", firstShape.hashCode(), equalTo(thirdShape.hashCode()));
            assertTrue("equals is not symmetric", thirdShape.equals(secondShape));
            assertTrue("equals is not symmetric", thirdShape.equals(firstShape));
        }
    }

    private SB copyShape(SB original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                ShapeBuilder prototype = (ShapeBuilder) namedWriteableRegistry.getPrototype(ShapeBuilder.class, original.getWriteableName());
                @SuppressWarnings("unchecked")
                SB copy = (SB) prototype.readFrom(in);
                return copy;
            }
        }
    }
}
