/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class CartesianShapeValuesGenericWriteableTests extends AbstractWireTestCase<
    CartesianShapeValuesGenericWriteableTests.GenericWriteableWrapper> {

    /**
     * Wrapper around a CartesianShapeValue to verify that it round-trips via {@code writeGenericValue} and {@code readGenericValue}
     */
    public record GenericWriteableWrapper(CartesianShapeValues.CartesianShapeValue shapeValue) implements Writeable {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(shapeValue);
        }

        public static GenericWriteableWrapper readFrom(StreamInput in) throws IOException {
            return new GenericWriteableWrapper((CartesianShapeValues.CartesianShapeValue) in.readGenericValue());
        }
    }

    private static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(
                GenericNamedWriteable.class,
                CartesianShapeValues.CartesianShapeValue.class.getSimpleName(),
                CartesianShapeValues.CartesianShapeValue::new
            )
        )
    );

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return NAMED_WRITEABLE_REGISTRY;
    }

    @Override
    protected GenericWriteableWrapper createTestInstance() {
        try {
            GeoBoundingBox bbox = GeoTestUtils.randomBBox();
            Rectangle rectangle = new Rectangle(bbox.left(), bbox.right(), bbox.top(), bbox.bottom());
            CartesianShapeValues.CartesianShapeValue shapeValue = GeoTestUtils.cartesianShapeValue(rectangle);
            return new GenericWriteableWrapper(shapeValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected GenericWriteableWrapper mutateInstance(GenericWriteableWrapper instance) throws IOException {
        CartesianShapeValues.CartesianShapeValue shapeValue = instance.shapeValue;
        ShapeValues.BoundingBox bbox = shapeValue.boundingBox();
        double height = bbox.maxY() - bbox.minY();
        double width = bbox.maxX() - bbox.minX();
        double xs = width * 0.001;
        double ys = height * 0.001;
        Rectangle rectangle = new Rectangle(bbox.minX() + xs, bbox.maxX() - xs, bbox.maxY() - ys, bbox.minY() + ys);
        return new GenericWriteableWrapper(GeoTestUtils.cartesianShapeValue(rectangle));
    }

    @Override
    protected GenericWriteableWrapper copyInstance(GenericWriteableWrapper instance, TransportVersion version) throws IOException {
        return copyInstance(instance, writableRegistry(), StreamOutput::writeWriteable, GenericWriteableWrapper::readFrom, version);
    }

    public void testSerializationFailsWithOlderVersion() {
        TransportVersion older = TransportVersions.KNN_AS_QUERY_ADDED;
        assert older.before(TransportVersions.SHAPE_VALUE_SERIALIZATION_ADDED);
        final var testInstance = createTestInstance().shapeValue();
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(older);
            assertThat(
                expectThrows(Throwable.class, () -> output.writeGenericValue(testInstance)).getMessage(),
                containsString("[CartesianShapeValue] requires minimal transport version")
            );
        }
    }
}
