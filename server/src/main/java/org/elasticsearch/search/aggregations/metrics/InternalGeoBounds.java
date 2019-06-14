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


    final double top;
    final double bottom;
    final double posLeft;
    final double posRight;
    final double negLeft;
    final double negRight;
    final boolean wrapLongitude;

    InternalGeoBounds(String name, double top, double bottom, double posLeft, double posRight,
                      double negLeft, double negRight, boolean wrapLongitude,
                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.top = top;
        this.bottom = bottom;
        this.posLeft = posLeft;
        this.posRight = posRight;
        this.negLeft = negLeft;
        this.negRight = negRight;
        this.wrapLongitude = wrapLongitude;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoBounds(StreamInput in) throws IOException {
        super(in);
        top = in.readDouble();
        bottom = in.readDouble();
        posLeft = in.readDouble();
        posRight = in.readDouble();
        negLeft = in.readDouble();
        negRight = in.readDouble();
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(top);
        out.writeDouble(bottom);
        out.writeDouble(posLeft);
        out.writeDouble(posRight);
        out.writeDouble(negLeft);
        out.writeDouble(negRight);
        out.writeBoolean(wrapLongitude);
    }

    @Override
    public String getWriteableName() {
        return GeoBoundsAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;

        for (InternalAggregation aggregation : aggregations) {
            InternalGeoBounds bounds = (InternalGeoBounds) aggregation;

            if (bounds.top > top) {
                top = bounds.top;
            }
            if (bounds.bottom < bottom) {
                bottom = bounds.bottom;
            }
            if (bounds.posLeft < posLeft) {
                posLeft = bounds.posLeft;
            }
            if (bounds.posRight > posRight) {
                posRight = bounds.posRight;
            }
            if (bounds.negLeft < negLeft) {
                negLeft = bounds.negLeft;
            }
            if (bounds.negRight > negRight) {
                negRight = bounds.negRight;
            }
        }
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, pipelineAggregators(),
                getMetaData());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            BoundingBox boundingBox = resolveBoundingBox();
            String bBoxSide = path.get(0);
            switch (bBoxSide) {
            case "top":
                return boundingBox.topLeft.lat();
            case "left":
                return boundingBox.topLeft.lon();
            case "bottom":
                return boundingBox.bottomRight.lat();
            case "right":
                return boundingBox.bottomRight.lon();
            default:
                throw new IllegalArgumentException("Found unknown path element [" + bBoxSide + "] in [" + getName() + "]");
            }
        } else if (path.size() == 2) {
            BoundingBox boundingBox = resolveBoundingBox();
            GeoPoint cornerPoint = null;
            String cornerString = path.get(0);
            switch (cornerString) {
            case "top_left":
                cornerPoint = boundingBox.topLeft;
                break;
            case "bottom_right":
                cornerPoint = boundingBox.bottomRight;
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

    private static class BoundingBox {
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        BoundingBox(GeoPoint topLeft, GeoPoint bottomRight) {
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        public GeoPoint topLeft() {
            return topLeft;
        }

        public GeoPoint bottomRight() {
            return bottomRight;
        }
    }

    private BoundingBox resolveBoundingBox() {
        if (Double.isInfinite(top)) {
            return null;
        } else if (Double.isInfinite(posLeft)) {
            return new BoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, negRight));
        } else if (Double.isInfinite(negLeft)) {
            return new BoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, posRight));
        } else if (wrapLongitude) {
            double unwrappedWidth = posRight - negLeft;
            double wrappedWidth = (180 - posLeft) - (-180 - negRight);
            if (unwrappedWidth <= wrappedWidth) {
                return new BoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
            } else {
                return new BoundingBox(new GeoPoint(top, posLeft), new GeoPoint(bottom, negRight));
            }
        } else {
            return new BoundingBox(new GeoPoint(top, negLeft), new GeoPoint(bottom, posRight));
        }
    }

    @Override
    public GeoPoint topLeft() {
        BoundingBox boundingBox = resolveBoundingBox();
        if (boundingBox == null) {
            return null;
        } else {
            return boundingBox.topLeft();
        }
    }

    @Override
    public GeoPoint bottomRight() {
        BoundingBox boundingBox = resolveBoundingBox();
        if (boundingBox == null) {
            return null;
        } else {
            return boundingBox.bottomRight();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalGeoBounds other = (InternalGeoBounds) obj;
        return top == other.top &&
            bottom == other.bottom &&
            posLeft == other.posLeft &&
            posRight == other.posRight &&
            negLeft == other.negLeft &&
            negRight == other.negRight &&
            wrapLongitude == other.wrapLongitude;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bottom, posLeft, posRight, negLeft, negRight, wrapLongitude);
    }
}
