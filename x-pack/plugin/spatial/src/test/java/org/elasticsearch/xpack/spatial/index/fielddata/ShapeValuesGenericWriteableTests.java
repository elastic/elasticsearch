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
     * A value received from another node is read in {@link ShapeValues.ShapeValue#RECEIVED_FORMAT} whatever format it was
     * read in on the sending node, so its centroid, shape type and extent must decode the same in both formats.
     */
    public void testReceivedValueKeepsHeader() throws IOException {
        ShapeValues.ShapeValue sent = randomShapeValue(randomFrom(GeometryDocValueFormat.values()));
        ShapeValues.ShapeValue received = copyInstance(new GenericWriteableWrapper(sent), TransportVersionUtils.randomCompatibleVersion())
            .shapeValue();
        assertEquals(sent, received);
        assertEquals(sent.getX(), received.getX(), 0);
        assertEquals(sent.getY(), received.getY(), 0);
        assertEquals(sent.dimensionalShapeType(), received.dimensionalShapeType());
        ShapeValues.BoundingBox sentBox = sent.boundingBox();
        ShapeValues.BoundingBox receivedBox = received.boundingBox();
        assertEquals(sentBox.top, receivedBox.top, 0);
        assertEquals(sentBox.bottom, receivedBox.bottom, 0);
        assertEquals(sentBox.negLeft, receivedBox.negLeft, 0);
        assertEquals(sentBox.negRight, receivedBox.negRight, 0);
        assertEquals(sentBox.posLeft, receivedBox.posLeft, 0);
        assertEquals(sentBox.posRight, receivedBox.posRight, 0);
    }
}
