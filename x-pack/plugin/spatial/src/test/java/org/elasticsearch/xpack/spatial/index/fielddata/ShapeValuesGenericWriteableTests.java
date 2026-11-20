/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.lucene.spatial.GeometryDocValueFormat;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public abstract class ShapeValuesGenericWriteableTests<T extends ShapeValues.ShapeValue> extends AbstractWireTestCase<
    ShapeValuesGenericWriteableTests.GenericWriteableWrapper> {

    private static final TransportVersion SHAPE_VALUE_DOC_VALUE_FORMAT = TransportVersion.fromName("shape_value_doc_value_format");

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

    @Override
    protected final GenericWriteableWrapper createTestInstance() {
        try {
            return new GenericWriteableWrapper(randomShapeValue(randomFrom(GeometryDocValueFormat.values())));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract String shapeValueName();

    /** A random shape value read from doc values in the given format. */
    protected abstract ShapeValues.ShapeValue randomShapeValue(GeometryDocValueFormat format) throws IOException;

    /**
     * Nodes that predate the doc-value format on the wire only read legacy doc values, so legacy values are sent to them
     * as before, and sending them V2 values fails rather than having them misread the bytes.
     */
    public void testSerializationBeforeDocValueFormat() throws IOException {
        TransportVersion version = randomFrom(
            TransportVersionUtils.allReleasedVersions()
                .stream()
                .filter(v -> v.supports(TransportVersion.minimumCompatible()) && v.supports(SHAPE_VALUE_DOC_VALUE_FORMAT) == false)
                .toList()
        );
        GenericWriteableWrapper legacy = new GenericWriteableWrapper(randomShapeValue(GeometryDocValueFormat.LEGACY));
        assertEquals(legacy, copyInstance(legacy, version));

        GenericWriteableWrapper v2 = new GenericWriteableWrapper(randomShapeValue(GeometryDocValueFormat.V2));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(v2, version));
        assertThat(e.getMessage(), containsString("which only reads the legacy format"));
    }
}
