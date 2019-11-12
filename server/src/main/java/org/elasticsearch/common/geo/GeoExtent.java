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
 * An extent computes the bounds of the provided points
 * or other extents.
 */
public class GeoExtent implements Writeable {

    private double top;
    private double bottom;
    private double negLeft;
    private double negRight;
    private double posLeft;
    private double posRight;

    public GeoExtent() {
        this.top = Double.NEGATIVE_INFINITY;
        this.bottom = Double.POSITIVE_INFINITY;
        this.negLeft = Double.POSITIVE_INFINITY;
        this.negRight = Double.NEGATIVE_INFINITY;
        this.posLeft = Double.POSITIVE_INFINITY;
        this.posRight = Double.NEGATIVE_INFINITY;
    }

    public GeoExtent(StreamInput input) throws IOException {
        this.top = input.readDouble();
        this.bottom = input.readDouble();
        this.negLeft = input.readDouble();
        this.negRight = input.readDouble();
        this.posLeft = input.readDouble();
        this.posRight = input.readDouble();
    }

    /**
     * Adds a point to the Extent
     */
    public void addPoint(double lat, double lon) {
        this.top = Math.max(this.top, lat);
        this.bottom = Math.min(this.bottom, lat);
        if (lon < 0) {
            this.negLeft = Math.min(this.negLeft, lon);
            this.negRight = Math.max(this.negRight, lon);
        } else {
            this.posLeft = Math.min(this.posLeft, lon);
            this.posRight = Math.max(this.posRight, lon);
        }
    }

    /**
     * Adds another extent to this extent.
     */
    public void addExtent(GeoExtent extent) {
        this.top = Math.max(this.top, extent.top);
        this.bottom = Math.min(this.bottom, extent.bottom);
        this.negLeft = Math.min(this.negLeft, extent.negLeft);
        this.negRight = Math.max(this.negRight, extent.negRight);
        this.posLeft = Math.min(this.posLeft, extent.posLeft);
        this.posRight = Math.max(this.posRight, extent.posRight);
    }

    /**
     * @return the minimum latitude of the extent
     * If no bounds, then {@link Double#POSITIVE_INFINITY} will be returned.
     */
    public double minLat() {
        return bottom;
    }

    /**
     * @return the maximum latitude of the extent
     * If no bounds, then {@link Double#NEGATIVE_INFINITY} will be returned.
     */
    public double maxLat() {
        return top;
    }

    /**
     * @return the absolute minimum longitude of the extent, whether it is positive or negative.
     * If no bounds, then {@link Double#POSITIVE_INFINITY} will be returned.
     */
    public double minLon(boolean wrap) {
        if (Double.isInfinite(posLeft)) {
            return negLeft;
        } else if (Double.isInfinite(negLeft)) {
            return posLeft;
        } else if (wrap) {
            double unwrappedWidth = posRight - negLeft;
            double wrappedWidth = (180 - posLeft) - (-180 - negRight);
            if (unwrappedWidth <= wrappedWidth) {
                return negLeft;
            } else {
                return posLeft;
            }
        } else {
            return negLeft;
        }
    }

    /**
     * @return the absolute maximum longitude of the extent, whether it is positive or negative.
     * If no bounds, then {@link Double#NEGATIVE_INFINITY} will be returned.
     */
    public double maxLon(boolean wrap) {
        if (Double.isInfinite(posLeft)) {
            return negRight;
        } else if (Double.isInfinite(negLeft)) {
            return posRight;
        } else if (wrap) {
            double unwrappedWidth = posRight - negLeft;
            double wrappedWidth = (180 - posLeft) - (-180 - negRight);
            if (unwrappedWidth <= wrappedWidth) {
                return posRight;
            } else {
                return negRight;
            }
        } else {
            return posRight;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(top);
        out.writeDouble(bottom);
        out.writeDouble(negLeft);
        out.writeDouble(negRight);
        out.writeDouble(posLeft);
        out.writeDouble(posRight);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoExtent extent = (GeoExtent) o;
        return top == extent.top &&
            bottom == extent.bottom &&
            negLeft == extent.negLeft &&
            negRight == extent.negRight &&
            posLeft == extent.posLeft &&
            posRight == extent.posRight;
    }

    @Override
    public int hashCode() {
        return Objects.hash(top, bottom, negLeft, negRight, posLeft, posRight);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder("[");
        buffer.append("top : " + top + ",");
        buffer.append("bottom : " + bottom + ",");
        buffer.append("negLeft : " + negLeft + ",");
        buffer.append("negRight : " + negRight + ",");
        buffer.append("posLeft : " + posLeft + ",");
        buffer.append("posRight : " + posRight + "]");
        return buffer.toString();
    }
}
