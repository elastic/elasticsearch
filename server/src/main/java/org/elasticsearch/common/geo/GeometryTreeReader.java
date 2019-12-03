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
import org.elasticsearch.geometry.ShapeType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * A reusable tree reader.
 *
 * This class supports checking bounding box
 * relations against the serialized geometry tree.
 */
public class GeometryTreeReader implements ShapeTreeReader {

    private static final int EXTENT_OFFSET = 8;
    private int startPosition;
    private ByteBufferStreamInput input;
    private final CoordinateEncoder coordinateEncoder;

    public GeometryTreeReader(CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
    }

    private GeometryTreeReader(ByteBufferStreamInput input, CoordinateEncoder coordinateEncoder) throws IOException {
        this.input = input;
        startPosition = input.position();
        this.coordinateEncoder = coordinateEncoder;
    }

    public void reset(BytesRef bytesRef) {
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        this.startPosition = 0;
    }

    @Override
    public double getCentroidX() throws IOException {
        input.position(startPosition);
        return coordinateEncoder.decodeX(input.readInt());
    }

    @Override
    public double getCentroidY() throws IOException {
        input.position(startPosition + 4);
        return coordinateEncoder.decodeY(input.readInt());
    }

    @Override
    public Extent getExtent() throws IOException {
        input.position(startPosition + EXTENT_OFFSET);
        Extent extent = input.readOptionalWriteable(Extent::new);
        if (extent != null) {
            return extent;
        }
        int numShapes = input.readVInt();
        assert numShapes == 1;
        // read ShapeType ordinal to avoid readEnum allocations
        int shapeTypeOrdinal = input.readVInt();
        ShapeTreeReader reader = getReader(shapeTypeOrdinal, coordinateEncoder, input);
        return reader.getExtent();
    }

    @Override
    public GeoRelation relate(Extent extent) throws IOException {
        GeoRelation relation = GeoRelation.QUERY_DISJOINT;
        input.position(startPosition + EXTENT_OFFSET);
        boolean hasExtent = input.readBoolean();
        if (hasExtent) {
            Optional<Boolean> extentCheck = EdgeTreeReader.checkExtent(new Extent(input), extent);
            if (extentCheck.isPresent()) {
                return extentCheck.get() ? GeoRelation.QUERY_INSIDE : GeoRelation.QUERY_DISJOINT;
            }
        }

        int numTrees = input.readVInt();
        int nextPosition = input.position();
        for (int i = 0; i < numTrees; i++) {
            if (numTrees > 1) {
                if (i > 0) {
                    input.position(nextPosition);
                }
                int pos = input.readVInt();
                nextPosition = input.position() + pos;
            }
            // read ShapeType ordinal to avoid readEnum allocations
            int shapeTypeOrdinal = input.readVInt();
            ShapeTreeReader reader = getReader(shapeTypeOrdinal, coordinateEncoder, input);
            GeoRelation shapeRelation = reader.relate(extent);
            if (GeoRelation.QUERY_CROSSES == shapeRelation ||
                (GeoRelation.QUERY_DISJOINT == shapeRelation && GeoRelation.QUERY_INSIDE == relation)
            ) {
                return GeoRelation.QUERY_CROSSES;
            } else {
                relation = shapeRelation;
            }
        }

        return relation;
    }

    private static ShapeTreeReader getReader(int shapeTypeOrdinal, CoordinateEncoder coordinateEncoder, ByteBufferStreamInput input)
            throws IOException {
        if (shapeTypeOrdinal == ShapeType.POLYGON.ordinal()) {
            return new PolygonTreeReader(input);
        } else if (shapeTypeOrdinal == ShapeType.POINT.ordinal() || shapeTypeOrdinal == ShapeType.MULTIPOINT.ordinal()) {
            return new Point2DReader(input);
        } else if (shapeTypeOrdinal == ShapeType.LINESTRING.ordinal() || shapeTypeOrdinal == ShapeType.MULTILINESTRING.ordinal()) {
            return new EdgeTreeReader(input, false);
        } else if (shapeTypeOrdinal == ShapeType.GEOMETRYCOLLECTION.ordinal()) {
            return new GeometryTreeReader(input, coordinateEncoder);
        }
        throw new UnsupportedOperationException("unsupported shape type ordinal [" + shapeTypeOrdinal + "]");
    }
}
