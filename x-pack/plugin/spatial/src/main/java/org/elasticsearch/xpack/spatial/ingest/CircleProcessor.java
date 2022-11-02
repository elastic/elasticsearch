/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeometryParserFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.CircleUtils;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

/**
 *  The circle-processor converts a circle shape definition into a valid regular polygon approximating the circle.
 */
public final class CircleProcessor extends AbstractProcessor {
    public static final String TYPE = "circle";

    private final String field;
    private final String targetField;
    private final boolean ignoreMissing;
    private final double errorDistance;
    private final CircleShapeFieldType circleShapeFieldType;

    CircleProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean ignoreMissing,
        double errorDistance,
        CircleShapeFieldType circleShapeFieldType
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.errorDistance = errorDistance;
        this.circleShapeFieldType = circleShapeFieldType;
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
        if (obj instanceof Map || obj instanceof String) {
            valueWrapper = Map.of("shape", obj);
        } else {
            throw new IllegalArgumentException("field [" + field + "] must be a WKT Circle or a GeoJSON Circle value");
        }

        MapXContentParser parser = new MapXContentParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            valueWrapper,
            XContentType.JSON
        );
        try {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // "shape" field key
            parser.nextToken(); // shape value
            GeometryParserFormat geometryFormat = GeometryParserFormat.geometryFormat(parser);
            Geometry geometry = geometryFormat.fromXContent(StandardValidator.instance(true), true, true, parser);
            if (ShapeType.CIRCLE.equals(geometry.type())) {
                Circle circle = (Circle) geometry;
                int numSides = CircleUtils.circleToPolygonNumSides(circle.getRadiusMeters(), errorDistance);
                final Geometry polygonizedCircle = switch (circleShapeFieldType) {
                    case GEO_SHAPE -> CircleUtils.createRegularGeoShapePolygon(circle, numSides);
                    case SHAPE -> CircleUtils.createRegularShapePolygon(circle, numSides);
                };
                XContentBuilder newValueBuilder = XContentFactory.jsonBuilder().startObject().field("val");
                geometryFormat.toXContent(polygonizedCircle, newValueBuilder, ToXContent.EMPTY_PARAMS);
                newValueBuilder.endObject();
                Map<String, Object> newObj = XContentHelper.convertToMap(BytesReference.bytes(newValueBuilder), true, XContentType.JSON)
                    .v2();
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

    double errorDistance() {
        return errorDistance;
    }

    CircleShapeFieldType shapeType() {
        return circleShapeFieldType;
    }

    public static final class Factory implements Processor.Factory {

        public CircleProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            double radiusDistance = Math.abs(ConfigurationUtils.readDoubleProperty(TYPE, processorTag, config, "error_distance"));
            CircleShapeFieldType circleFieldType = CircleShapeFieldType.parse(
                ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "shape_type")
            );
            return new CircleProcessor(processorTag, description, field, targetField, ignoreMissing, radiusDistance, circleFieldType);
        }
    }

    enum CircleShapeFieldType {
        SHAPE,
        GEO_SHAPE;

        public static CircleShapeFieldType parse(String value) {
            EnumSet<CircleShapeFieldType> validValues = EnumSet.allOf(CircleShapeFieldType.class);
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "illegal [shape_type] value [" + value + "]. valid values are " + Arrays.toString(validValues.toArray())
                );
            }
        }
    }
}
