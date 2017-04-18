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

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public abstract class AbstractShapeBuilderTestCase<SB extends ShapeBuilder> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        if (namedWriteableRegistry == null) {
            List<NamedWriteableRegistry.Entry> shapes = new ArrayList<>();
            ShapeBuilders.register(shapes);
            namedWriteableRegistry = new NamedWriteableRegistry(shapes);
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /**
     * create random shape that is put under test
     */
    protected abstract SB createTestShapeBuilder();

    /**
     * mutate the given shape so the returned shape is different
     */
    protected abstract SB createMutation(SB original) throws IOException;

    /**
     * Test that creates new shape from a random test shape and checks both for equality
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB testShape = createTestShapeBuilder();
            XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                contentBuilder.prettyPrint();
            }
            XContentBuilder builder = testShape.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);
            XContentParser shapeParser = createParser(shuffled);
            shapeParser.nextToken();
            ShapeBuilder parsedShape = ShapeBuilder.parse(shapeParser);
            assertNotSame(testShape, parsedShape);
            assertEquals(testShape, parsedShape);
            assertEquals(testShape.hashCode(), parsedShape.hashCode());
        }
    }

    /**
     * Test serialization and deserialization of the test shape.
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB testShape = createTestShapeBuilder();
            SB deserializedShape = copyShape(testShape);
            assertEquals(testShape, deserializedShape);
            assertEquals(testShape.hashCode(), deserializedShape.hashCode());
            assertNotSame(testShape, deserializedShape);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(createTestShapeBuilder(), AbstractShapeBuilderTestCase::copyShape, this::createMutation);
        }
    }

    protected static <T extends NamedWriteable> T copyShape(T original) throws IOException {
        @SuppressWarnings("unchecked")
        Reader<T> reader = (Reader<T>) namedWriteableRegistry.getReader(ShapeBuilder.class, original.getWriteableName());
        return ESTestCase.copyWriteable(original, namedWriteableRegistry, reader);
    }
}
