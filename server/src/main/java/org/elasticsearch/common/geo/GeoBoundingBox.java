/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * A class representing a Geo-Bounding-Box for use by Geo queries and aggregations
 * that deal with extents/rectangles representing rectangular areas of interest.
 */
public class GeoBoundingBox extends BoundingBox<GeoPoint> {
    public static final ParseField LAT_FIELD = new ParseField("lat");
    public static final ParseField LON_FIELD = new ParseField("lon");

    public GeoBoundingBox(GeoPoint topLeft, GeoPoint bottomRight) {
        super(topLeft, bottomRight);
    }

    public GeoBoundingBox(StreamInput input) throws IOException {
        super(input.readGeoPoint(), input.readGeoPoint());
    }

    @Override
    public GeoPoint topLeft() {
        return topLeft;
    }

    @Override
    public GeoPoint bottomRight() {
        return bottomRight;
    }

    @Override
    public XContentBuilder toXContentFragment(XContentBuilder builder) throws IOException {
        builder.startObject(TOP_LEFT_FIELD.getPreferredName());
        builder.field(LAT_FIELD.getPreferredName(), topLeft.getY());
        builder.field(LON_FIELD.getPreferredName(), topLeft.getX());
        builder.endObject();
        builder.startObject(BOTTOM_RIGHT_FIELD.getPreferredName());
        builder.field(LAT_FIELD.getPreferredName(), bottomRight.getY());
        builder.field(LON_FIELD.getPreferredName(), bottomRight.getX());
        builder.endObject();
        return builder;
    }

    /**
     * There exists a special case where we use an array format for building the XContent for the bounds.
     * Specifically the GeoBoundingBoxQueryBuilder makes use of this. All other cases build a keyed map.
     */
    public XContentBuilder toXContentFragmentWithArray(XContentBuilder builder) throws IOException {
        builder.array(TOP_LEFT_FIELD.getPreferredName(), topLeft.getX(), topLeft.getY());
        builder.array(BOTTOM_RIGHT_FIELD.getPreferredName(), bottomRight.getX(), bottomRight.getY());
        return builder;
    }

    /**
     * If the bounding box crosses the date-line (left greater-than right) then the
     * longitude of the point need only to be higher than the left or lower
     * than the right. Otherwise, it must be both.
     *
     * @param lon the longitude of the point
     * @param lat the latitude of the point
     * @return whether the point (lon, lat) is in the specified bounding box
     */
    public boolean pointInBounds(double lon, double lat) {
        if (lat >= bottom() && lat <= top()) {
            if (left() <= right()) {
                return lon >= left() && lon <= right();
            } else {
                return lon >= left() || lon <= right();
            }
        }
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGeoPoint(topLeft);
        out.writeGeoPoint(bottomRight);
    }

    protected static class GeoBoundsParser extends BoundsParser<GeoBoundingBox> {
        GeoBoundsParser(XContentParser parser) {
            super(parser);
        }

        @Override
        protected GeoBoundingBox createWithEnvelope() {
            GeoPoint topLeft = new GeoPoint(envelope.getMaxLat(), envelope.getMinLon());
            GeoPoint bottomRight = new GeoPoint(envelope.getMinLat(), envelope.getMaxLon());
            return new GeoBoundingBox(topLeft, bottomRight);
        }

        @Override
        protected GeoBoundingBox createWithBounds() {
            GeoPoint topLeft = new GeoPoint(top, left);
            GeoPoint bottomRight = new GeoPoint(bottom, right);
            return new GeoBoundingBox(topLeft, bottomRight);
        }

        @Override
        protected SpatialPoint parsePointWith(XContentParser parser, GeoUtils.EffectivePoint effectivePoint) throws IOException {
            return GeoUtils.parseGeoPoint(parser, false, effectivePoint);
        }
    }

    /**
     * Parses the bounding box and returns bottom, top, left, right coordinates
     */
    public static GeoBoundingBox parseBoundingBox(XContentParser parser) throws IOException, ElasticsearchParseException {
        GeoBoundsParser bounds = new GeoBoundsParser(parser);
        return bounds.parseBoundingBox();
    }
}
