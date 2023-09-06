/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class GeoBoundsGenericWriteableTests extends AbstractNamedWriteableTestCase<GenericNamedWriteable> {
    NamedWriteableRegistry registry = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(GenericNamedWriteable.class, GeoBoundingBox.class.getSimpleName(), GeoBoundingBox::new))
    );

    @Override
    protected GeoBoundingBox createTestInstance() {
        Rectangle box = GeoTestUtil.nextBox();
        return new GeoBoundingBox(new GeoPoint(box.maxLat, box.minLon), new GeoPoint(box.minLat, box.maxLon));
    }

    @Override
    protected GeoBoundingBox mutateInstance(GenericNamedWriteable instance) throws IOException {
        assert instance instanceof GeoBoundingBox : "Expected GeoBoundingBox";
        GeoBoundingBox geoBoundingBox = (GeoBoundingBox) instance;
        double width = geoBoundingBox.right() - geoBoundingBox.left();
        double height = geoBoundingBox.top() - geoBoundingBox.bottom();
        double top = geoBoundingBox.top() - height / 4;
        double left = geoBoundingBox.left() + width / 4;
        double bottom = geoBoundingBox.bottom() + height / 4;
        double right = geoBoundingBox.right() - width / 4;
        return new GeoBoundingBox(new GeoPoint(top, left), new GeoPoint(bottom, right));
    }

    @Override
    protected GenericNamedWriteable copyInstance(GenericNamedWriteable original, TransportVersion version) throws IOException {
        return copyInstance(original, version, version);
    }

    /**
     * Read and write with different serialization versions
     */
    protected GenericNamedWriteable copyInstance(
        GenericNamedWriteable original,
        TransportVersion writeVersion,
        TransportVersion readVersion
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(writeVersion);
            output.writeGenericValue(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                in.setTransportVersion(readVersion);
                return readGenericValue(in);
            }
        }
    }

    private GenericNamedWriteable readGenericValue(StreamInput in) throws IOException {
        Object obj = in.readGenericValue();
        if (obj instanceof GenericNamedWriteable result) {
            return result;
        }
        throw new IllegalStateException("Read result of wrong type: " + obj.getClass().getSimpleName());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return registry;
    }

    @Override
    protected Class<GenericNamedWriteable> categoryClass() {
        return GenericNamedWriteable.class;
    }

    public void testSerializationFailsWithOlderVersion() throws IOException {
        TransportVersion valid = TransportVersion.V_8_500_070;
        TransportVersion older = TransportVersion.V_8_500_069;
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            GenericNamedWriteable testInstance = createTestInstance();

            // Test reading and writing with valid versions
            GenericNamedWriteable deserializedInstance = copyInstance(testInstance, valid, valid);
            assertEqualInstances(testInstance, deserializedInstance);

            // Test writing fails with older version
            try {
                copyInstance(testInstance, older, valid);
                fail("Expected exception to be thrown when writing with older TransportVersion");
            } catch (Throwable e) {
                assertThat(
                    "Writing with older TransportVersion",
                    e.getMessage(),
                    containsString("[GeoBoundingBox] requires minimal transport version")
                );
            }

            // Test reading succeeds with older version (we do not cripple the reader)
            deserializedInstance = copyInstance(testInstance, valid, older);
            assertEqualInstances(testInstance, deserializedInstance);
        }
    }
}
