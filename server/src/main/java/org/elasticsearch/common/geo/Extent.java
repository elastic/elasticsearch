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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Object representing the extent of a geometry object within a
 * {@link GeometryTreeWriter} and {@link EdgeTreeWriter};
 */
public final class Extent implements Writeable {
    public final int minX;
    public final int minY;
    public final int maxX;
    public final int maxY;

    Extent(int minX, int minY, int maxX, int maxY) {
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
    }

    Extent(StreamInput input) throws IOException {
        this(input.readInt(), input.readInt(), input.readInt(), input.readInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(minX);
        out.writeInt(minY);
        out.writeInt(maxX);
        out.writeInt(maxY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Extent extent = (Extent) o;
        return minX == extent.minX &&
            minY == extent.minY &&
            maxX == extent.maxX &&
            maxY == extent.maxY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minX, minY, maxX, maxY);
    }
}
