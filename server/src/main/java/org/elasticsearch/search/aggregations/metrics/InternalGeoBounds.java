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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoExtent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalGeoBounds extends InternalAggregation implements GeoBounds {

    static final ParseField BOUNDS_FIELD = new ParseField("bounds");
    static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");
    static final ParseField LAT_FIELD = new ParseField("lat");
    static final ParseField LON_FIELD = new ParseField("lon");


    final GeoExtent extent;
    final boolean wrapLongitude;

    InternalGeoBounds(String name, GeoExtent extent, boolean wrapLongitude,
                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.extent = extent;
        this.wrapLongitude = wrapLongitude;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoBounds(StreamInput in) throws IOException {
        super(in);
        extent = new GeoExtent(in);
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        extent.writeTo(out);
        out.writeBoolean(wrapLongitude);
    }

    @Override
    public String getWriteableName() {
        return GeoBoundsAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        GeoExtent extent = new GeoExtent();
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoBounds bounds = (InternalGeoBounds) aggregation;
            extent.addExtent(bounds.extent);
        }
        return new InternalGeoBounds(name, extent, wrapLongitude, pipelineAggregators(),
                getMetaData());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String bBoxSide = path.get(0);
            switch (bBoxSide) {
            case "top":
                return extent.maxLat();
            case "left":
                return extent.minLon(wrapLongitude);
            case "bottom":
                return extent.minLat();
            case "right":
                return extent.maxLon(wrapLongitude);
            default:
                throw new IllegalArgumentException("Found unknown path element [" + bBoxSide + "] in [" + getName() + "]");
            }
        } else if (path.size() == 2) {
            GeoPoint cornerPoint;
            String cornerString = path.get(0);
            switch (cornerString) {
            case "top_left":
                cornerPoint = topLeft();
                break;
            case "bottom_right":
                cornerPoint = bottomRight();
                break;
            default:
                throw new IllegalArgumentException("Found unknown path element [" + cornerString + "] in [" + getName() + "]");
            }
            String latLonString = path.get(1);
            switch (latLonString) {
            case "lat":
                return cornerPoint.lat();
            case "lon":
                return cornerPoint.lon();
            default:
                throw new IllegalArgumentException("Found unknown path element [" + latLonString + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        GeoPoint topLeft = topLeft();
        GeoPoint bottomRight = bottomRight();
        if (topLeft != null) {
            builder.startObject(BOUNDS_FIELD.getPreferredName());
            builder.startObject(TOP_LEFT_FIELD.getPreferredName());
            builder.field(LAT_FIELD.getPreferredName(), topLeft.lat());
            builder.field(LON_FIELD.getPreferredName(), topLeft.lon());
            builder.endObject();
            builder.startObject(BOTTOM_RIGHT_FIELD.getPreferredName());
            builder.field(LAT_FIELD.getPreferredName(), bottomRight.lat());
            builder.field(LON_FIELD.getPreferredName(), bottomRight.lon());
            builder.endObject();
            builder.endObject();
        }
        return builder;
    }

    @Override
    public GeoPoint topLeft() {
        double maxLat = extent.maxLat();
        if (maxLat == Double.NEGATIVE_INFINITY) {
            return null;
        } else {
            double minLon = extent.minLon(wrapLongitude);
            return new GeoPoint(maxLat, minLon);
        }
    }

    @Override
    public GeoPoint bottomRight() {
        double minLat = extent.minLat();
        if (minLat == Double.POSITIVE_INFINITY) {
            return null;
        } else {
            double maxLon = extent.maxLon(wrapLongitude);
            return new GeoPoint(minLat, maxLon);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalGeoBounds other = (InternalGeoBounds) obj;
        return extent.equals(other.extent) &&
            wrapLongitude == other.wrapLongitude;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), extent, wrapLongitude);
    }
}
