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

import org.elasticsearch.common.io.stream.ByteBufferStreamInput;

import java.io.IOException;

/**
 * This {@link ShapeTreeReader} understands how to parse polygons
 * serialized with the {@link PolygonTreeWriter}
 */
public class PolygonTreeReader implements ShapeTreeReader {
    private final EdgeTreeReader outerShell;
    private final EdgeTreeReader holes;

    public PolygonTreeReader(ByteBufferStreamInput input) throws IOException {
        int outerShellSize = input.readVInt();
        int outerShellPosition = input.position();
        this.outerShell = new EdgeTreeReader(input, true);
        input.position(outerShellPosition + outerShellSize);
        boolean hasHoles = input.readBoolean();
        if (hasHoles) {
            this.holes = new EdgeTreeReader(input, true);
        } else {
            this.holes = null;
        }
    }

    public Extent getExtent() throws IOException {
        return outerShell.getExtent();
    }

    @Override
    public double getCentroidX() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getCentroidY() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns true if the rectangle query and the edge tree's shape overlap
     */
    @Override
    public GeoRelation relate(int minX, int minY, int maxX, int maxY) throws IOException {
        if (holes != null) {
            GeoRelation relation = holes.relate(minX, minY, maxX, maxY);
            if (GeoRelation.QUERY_CROSSES == relation) {
                return GeoRelation.QUERY_CROSSES;
            }
            if (GeoRelation.QUERY_INSIDE == relation) {
                return GeoRelation.QUERY_DISJOINT;
            }
        }
        return outerShell.relate(minX, minY, maxX, maxY);
    }
}
