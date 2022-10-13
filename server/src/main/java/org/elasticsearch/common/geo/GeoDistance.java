/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    PLANE,
    ARC;

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
            return DistanceUnit.convert(GeoUtils.planeDistance(srcLat, srcLon, dstLat, dstLon), DistanceUnit.METERS, unit);
        }
        return DistanceUnit.convert(GeoUtils.arcDistance(srcLat, srcLon, dstLat, dstLon), DistanceUnit.METERS, unit);
    }
}
