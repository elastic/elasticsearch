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
        ShapeTreeReader reader = getReader(shapeType, input);
        return reader.getExtent();
    }

    public boolean intersects(Extent extent) throws IOException {
        input.position(0);
        boolean hasExtent = input.readBoolean();
        if (hasExtent) {
            Optional<Boolean> extentCheck = EdgeTreeReader.checkExtent(input, extent);
            if (extentCheck.isPresent()) {
                return extentCheck.get();
            }
        }

        int numTrees = input.readVInt();
        for (int i = 0; i < numTrees; i++) {
            ShapeType shapeType = input.readEnum(ShapeType.class);
            ShapeTreeReader reader = getReader(shapeType, input);
            if (reader.intersects(extent)) {
                return true;
            }
        }
        return false;
    }

    private static ShapeTreeReader getReader(ShapeType shapeType, ByteBufferStreamInput input) throws IOException {
        switch (shapeType) {
            case POLYGON:
                return new EdgeTreeReader(input);
            case POINT:
            case MULTIPOINT:
                return new Point2DReader(input);
            case LINESTRING:
            case MULTILINESTRING:
                throw new UnsupportedOperationException("TODO: linestring and multilinestring");
            default:
                throw new UnsupportedOperationException("unsupported shape type [" + shapeType + "]");
        }
    }
}
