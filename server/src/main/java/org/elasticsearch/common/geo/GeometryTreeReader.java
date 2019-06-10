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
package org.elasticsearch.common.geo;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.geo.geometry.ShapeType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * A tree reader.
 *
 * This class supports checking bounding box
 * relations against the serialized geometry tree.
 */
public class GeometryTreeReader {

    private final ByteBufferStreamInput input;

    public GeometryTreeReader(BytesRef bytesRef) {
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
    }

    public Extent getExtent() throws IOException {
        input.position(0);
        boolean hasExtent = input.readBoolean();
        if (hasExtent) {
            return new Extent(input);
        }
        assert input.readVInt() == 1;
        ShapeType shapeType = input.readEnum(ShapeType.class);
        if (ShapeType.POLYGON.equals(shapeType)) {
            EdgeTreeReader reader = new EdgeTreeReader(input);
            return reader.getExtent();
        } else {
            throw new UnsupportedOperationException("only polygons supported -- TODO");
        }
    }

    public boolean containedInOrCrosses(int minLon, int minLat, int maxLon, int maxLat) throws IOException {
        input.position(0);
        boolean hasExtent = input.readBoolean();
        if (hasExtent) {
            Optional<Boolean> extentCheck = EdgeTreeReader.checkExtent(input,
                new Extent(minLon, minLat, maxLon, maxLat));
            if (extentCheck.isPresent()) {
                return extentCheck.get();
            }
        }

        int numTrees = input.readVInt();
        for (int i = 0; i < numTrees; i++) {
            ShapeType shapeType = input.readEnum(ShapeType.class);
            if (ShapeType.POLYGON.equals(shapeType)) {
                EdgeTreeReader reader = new EdgeTreeReader(input);
                if (reader.containedInOrCrosses(minLon, minLat, maxLon, maxLat)) {
                    return true;
                }
            }
        }
        return false;
    }
}
