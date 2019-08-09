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

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.ShapeType;

import java.io.IOException;
import java.util.List;

/**
 * {@link Polygon} and {@link Rectangle} Shape Tree Writer for use in doc-values
 */
public class PolygonTreeWriter extends ShapeTreeWriter {

    private final EdgeTreeWriter outerShell;
    private final EdgeTreeWriter holes;

    public PolygonTreeWriter(int[] x, int[] y, List<int[]> holesX, List<int[]> holesY) {
        outerShell = new EdgeTreeWriter(x, y);
        holes = holesX.isEmpty() ? null : new EdgeTreeWriter(holesX, holesY);
    }

    public Extent getExtent() {
        return outerShell.getExtent();
    }

    public ShapeType getShapeType() {
        return ShapeType.POLYGON;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // calculate size of outerShell's tree to make it easy to jump to the holes tree quickly when querying
        int size = outerShell.tree.size * EdgeTreeWriter.EDGE_SIZE_IN_BYTES + Extent.WRITEABLE_SIZE_IN_BYTES + 1;
        out.writeVInt(size);
        long startPosition = out.position();
        outerShell.writeTo(out);
        assert out.position() == size + startPosition;
        out.writeOptionalWriteable(holes);
    }
}
