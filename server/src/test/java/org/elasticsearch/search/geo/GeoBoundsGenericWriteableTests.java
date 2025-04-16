/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.geo;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.geo.GeoBoundsGenericWriteableTests.GenericWriteableWrapper;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class GeoBoundsGenericWriteableTests extends AbstractWireTestCase<GenericWriteableWrapper> {

    /**
     * Wrapper around a GeoBoundingBox to verify that it round-trips via {@code writeGenericValue} and {@code readGenericValue}
     */
    public record GenericWriteableWrapper(GeoBoundingBox geoBoundingBox) implements Writeable {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericValue(geoBoundingBox);
        }

        public static GenericWriteableWrapper readFrom(StreamInput in) throws IOException {
            return new GenericWriteableWrapper((GeoBoundingBox) in.readGenericValue());
        }
    }

    private static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(GenericNamedWriteable.class, GeoBoundingBox.class.getSimpleName(), GeoBoundingBox::new))
    );

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return NAMED_WRITEABLE_REGISTRY;
    }

    @Override
    protected GenericWriteableWrapper createTestInstance() {
        Rectangle box = GeoTestUtil.nextBox();
        return new GenericWriteableWrapper(new GeoBoundingBox(new GeoPoint(box.maxLat, box.minLon), new GeoPoint(box.minLat, box.maxLon)));
    }

    @Override
    protected GenericWriteableWrapper mutateInstance(GenericWriteableWrapper instance) throws IOException {
        GeoBoundingBox geoBoundingBox = instance.geoBoundingBox;
        double width = geoBoundingBox.right() - geoBoundingBox.left();
        double height = geoBoundingBox.top() - geoBoundingBox.bottom();
        double top = geoBoundingBox.top() - height / 4;
        double left = geoBoundingBox.left() + width / 4;
        double bottom = geoBoundingBox.bottom() + height / 4;
        double right = geoBoundingBox.right() - width / 4;
        return new GenericWriteableWrapper(new GeoBoundingBox(new GeoPoint(top, left), new GeoPoint(bottom, right)));
    }

    @Override
    protected GenericWriteableWrapper copyInstance(GenericWriteableWrapper instance, TransportVersion version) throws IOException {
        return copyInstance(instance, writableRegistry(), StreamOutput::writeWriteable, GenericWriteableWrapper::readFrom, version);
    }

    public void testSerializationFailsWithOlderVersion() {
        TransportVersion older = TransportVersions.V_8_10_X;
        assert older.before(TransportVersions.V_8_11_X);
        final var testInstance = createTestInstance().geoBoundingBox();
        try (var output = new BytesStreamOutput()) {
            output.setTransportVersion(older);
            assertThat(
                expectThrows(Throwable.class, () -> output.writeGenericValue(testInstance)).getMessage(),
                containsString("[GeoBoundingBox] doesn't support serialization with transport version")
            );
        }
    }
}
