/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public abstract class ShapeValuesGenericWriteableTests<T extends ShapeValues.ShapeValue> extends AbstractWireTestCase<
    ShapeValuesGenericWriteableTests.GenericWriteableWrapper> {

    /**
     * Wrapper around a GeoShapeValue to verify that it round-trips via {@code writeGenericValue} and {@code readGenericValue}
     */
    public record GenericWriteableWrapper(ShapeValues.ShapeValue shapeValue) implements Writeable {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(shapeValue);
        }

        public static GenericWriteableWrapper readFrom(StreamInput in) throws IOException {
            return new GenericWriteableWrapper((ShapeValues.ShapeValue) in.readGenericValue());
        }
    }

    private static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(
                GenericNamedWriteable.class,
                GeoShapeValues.GeoShapeValue.class.getSimpleName(),
                GeoShapeValues.GeoShapeValue::new
            ),
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
    protected GenericWriteableWrapper copyInstance(GenericWriteableWrapper instance, TransportVersion version) throws IOException {
        return copyInstance(instance, writableRegistry(), StreamOutput::writeWriteable, GenericWriteableWrapper::readFrom, version);
    }

    protected abstract String shapeValueName();

    public void testSerializationFailsWithOlderVersion() {
        TransportVersion older = TransportVersions.V_8_11_X;
        final var testInstance = createTestInstance().shapeValue();
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(older);
            assertThat(
                expectThrows(Throwable.class, () -> output.writeGenericValue(testInstance)).getMessage(),
                containsString("[" + shapeValueName() + "] doesn't support serialization with transport version")
            );
        }
    }
}
