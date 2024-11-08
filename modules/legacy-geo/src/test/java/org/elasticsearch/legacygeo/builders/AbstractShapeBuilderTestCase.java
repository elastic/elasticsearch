/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public abstract class AbstractShapeBuilderTestCase<SB extends ShapeBuilder<?, ?, ?>> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        if (namedWriteableRegistry == null) {
            namedWriteableRegistry = new NamedWriteableRegistry(GeoShapeType.getShapeWriteables());
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
            try (XContentParser shapeContentParser = createParser(shuffled)) {
                shapeContentParser.nextToken();
                ShapeBuilder<?, ?, ?> parsedShape = ShapeParser.parse(shapeContentParser);
                assertNotSame(testShape, parsedShape);
                assertEquals(testShape, parsedShape);
                assertEquals(testShape.hashCode(), parsedShape.hashCode());
            }
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
