/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

/**
 *  The circle-processor converts a circle shape definition into a valid regular polygon approximating the circle.
 */
public final class CircleProcessor extends AbstractProcessor {
    public static final String TYPE = "circle";
    static final GeometryParser PARSER = new GeometryParser(true, true, true);
    static final int MINIMUM_NUMBER_OF_SIDES = 4;
    static final int MAXIMUM_NUMBER_OF_SIDES = 1000;

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final double errorDistanceMeters;

    CircleProcessor(String tag, String field, String targetField, boolean ignoreMissing, double errorDistanceMeters) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.errorDistanceMeters = errorDistanceMeters;
    }


    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument ingestDocument) {
        Object obj = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

        if (obj == null && ignoreMissing) {
            return ingestDocument;
        } else if (obj == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }

        final Map<String, Object> valueWrapper;
        if (obj instanceof Map) {
            Map<String, Object> fieldValue = (Map<String, Object>) obj;
            valueWrapper = Map.of("shape", fieldValue);
        } else if (obj instanceof String) {
            valueWrapper = Map.of("shape", obj);
        } else {
            throw new IllegalArgumentException("field [" + field + "] must be a WKT Circle or a GeoJSON Circle value");
        }

        MapXContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, valueWrapper, XContentType.JSON);
        try {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // "shape" field key
            parser.nextToken(); // shape value
            GeometryFormat geometryFormat = PARSER.geometryFormat(parser);
            Geometry geometry = geometryFormat.fromXContent(parser);
            if (ShapeType.CIRCLE.equals(geometry.type())) {
                Circle circle = (Circle) geometry;
                int numSides = numSides(circle.getRadiusMeters());
                Geometry polygonizedCircle = createRegularPolygon(circle.getLat(), circle.getLon(), circle.getRadiusMeters(), numSides);
                XContentBuilder newValueBuilder = XContentFactory.jsonBuilder().startObject().field("val");
                geometryFormat.toXContent(polygonizedCircle, newValueBuilder, ToXContent.EMPTY_PARAMS);
                newValueBuilder.endObject();
                Map<String, Object> newObj = XContentHelper.convertToMap(
                    BytesReference.bytes(newValueBuilder), true, XContentType.JSON).v2();
                ingestDocument.setFieldValue(targetField, newObj.get("val"));
            } else {
                throw new IllegalArgumentException("found [" + geometry.type() + "] instead of circle");
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid circle definition", e);
        }

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String field() {
        return field;
    }

    String targetField() {
        return targetField;
    }

    double errorDistanceMeters() {
        return errorDistanceMeters;
    }

    int numSides(double radiusMeters) {
        int val = (int) Math.ceil(2 * Math.PI / Math.acos(1 - errorDistanceMeters / radiusMeters));
        return Math.min(MAXIMUM_NUMBER_OF_SIDES, Math.max(MINIMUM_NUMBER_OF_SIDES, val));
    }


    /**
     * Makes an n-gon, centered at the provided lat/lon, and each vertex approximately
     * radiusMeters away from the center.
     *
     * This does not split the polygon across the date-line. Relies on {@link GeoShapeIndexer} to
     * split prepare polygon for indexing.
     *
     * Adapted from from org.apache.lucene.geo.GeoTestUtil
     * */
    public static Polygon createRegularPolygon(double centerLat, double centerLon, double radiusMeters, int gons) {
        double[][] result = new double[2][];
        result[0] = new double[gons+1];
        result[1] = new double[gons+1];
        for(int i=0;i<gons;i++) {
            double angle = i*(360.0/gons);
            double x = Math.cos(SloppyMath.toRadians(angle));
            double y = Math.sin(SloppyMath.toRadians(angle));
            double factor = 2.0;
            double step = 1.0;
            int last = 0;

            // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
            while (true) {
                double lat = centerLat + y * factor;
                double lon = centerLon + x * factor;
                double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

                if (Math.abs(distanceMeters - radiusMeters) < 0.1) {
                    // Within 10 cm: close enough!
                    result[0][i] = GeoUtils.normalizeLon(lon);
                    result[1][i] = GeoUtils.normalizeLat(lat);
                    break;
                }

                if (distanceMeters > radiusMeters) {
                    // too big
                    factor -= step;
                    if (last == 1) {
                        step /= 2.0;
                    }
                    last = -1;
                } else if (distanceMeters < radiusMeters) {
                    // too small
                    factor += step;
                    if (last == -1) {
                        step /= 2.0;
                    }
                    last = 1;
                }
            }
        }

        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];
        return new Polygon(new LinearRing(result[0], result[1]));
    }

    public static final class Factory implements Processor.Factory {

        public CircleProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            double radiusDistanceMeters = Math.abs(ConfigurationUtils.readDoubleProperty(TYPE, processorTag, config,
                "error_distance_in_meters"));
            return new CircleProcessor(processorTag, field, targetField, ignoreMissing, radiusDistanceMeters);
        }
    }
}
