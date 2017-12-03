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
import org.elasticsearch.common.unit.DistanceUnit;

import java.io.IOException;
import java.util.Locale;

/**
 * Geo distance calculation.
 */
public enum GeoDistance implements Writeable {
    PLANE, ARC;

    /** Creates a GeoDistance instance from an input stream */
    public static GeoDistance readFromStream(StreamInput in) throws IOException {
        int ord = in.readVInt();
        if (ord < 0 || ord >= values().length) {
            throw new IOException("Unknown GeoDistance ordinal [" + ord + "]");
        }
        return GeoDistance.values()[ord];
    }

    /** Writes an instance of a GeoDistance object to an output stream */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal());
    }

    /**
     * Get a {@link GeoDistance} according to a given name. Valid values are
     *
     * <ul>
     *     <li><b>plane</b> for <code>GeoDistance.PLANE</code></li>
     *     <li><b>arc</b> for <code>GeoDistance.ARC</code></li>
     * </ul>
     *
     * @param name name of the {@link GeoDistance}
     * @return a {@link GeoDistance}
     */
    public static GeoDistance fromString(String name) {
        name = name.toLowerCase(Locale.ROOT);
        if ("plane".equals(name)) {
            return PLANE;
        } else if ("arc".equals(name)) {
            return ARC;
        }
        throw new IllegalArgumentException("No geo distance for [" + name + "]");
    }

    /** compute the distance between two points using the selected algorithm (PLANE, ARC) */
    public double calculate(double srcLat, double srcLon, double dstLat, double dstLon, DistanceUnit unit) {
        if (this == PLANE) {
            return DistanceUnit.convert(GeoUtils.planeDistance(srcLat, srcLon, dstLat, dstLon),
                DistanceUnit.METERS, unit);
        }
        return DistanceUnit.convert(GeoUtils.arcDistance(srcLat, srcLon, dstLat, dstLon), DistanceUnit.METERS, unit);
    }
}
