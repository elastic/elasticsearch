/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.geo.parsers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.index.mapper.AbstractShapeGeometryFieldMapper;

import java.io.IOException;
import java.util.Collections;

/**
 * first point of entry for a shape parser
 */
public interface ShapeParser {
    ParseField FIELD_TYPE = new ParseField("type");
    ParseField FIELD_COORDINATES = new ParseField("coordinates");
    ParseField FIELD_GEOMETRIES = new ParseField("geometries");
    ParseField FIELD_ORIENTATION = new ParseField("orientation");

    /**
     * Create a new {@link ShapeBuilder} from {@link XContent}
     * @param parser parser to read the GeoShape from
     * @param geometryMapper document field mapper reference required for spatial parameters relevant
     *                     to the shape construction process (e.g., orientation)
     *                     todo: refactor to place build specific parameters in the SpatialContext
     * @return {@link ShapeBuilder} read from the parser or null
     *          if the parsers current token has been <code>null</code>
     * @throws IOException if the input could not be read
     */
    static ShapeBuilder<?, ?, ?> parse(XContentParser parser, AbstractGeometryFieldMapper<?> geometryMapper) throws IOException {
        AbstractShapeGeometryFieldMapper<?> shapeMapper = null;
        if (geometryMapper != null) {
            if (geometryMapper instanceof AbstractShapeGeometryFieldMapper == false) {
                throw new IllegalArgumentException("geometry must be a shape type");
            }
             shapeMapper = (AbstractShapeGeometryFieldMapper<?>) geometryMapper;
        }
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        } if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return GeoJsonParser.parse(parser, shapeMapper);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return GeoWKTParser.parse(parser, shapeMapper);
        }
        throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
    }

    /**
     * Create a new {@link ShapeBuilder} from {@link XContent}
     * @param parser parser to read the GeoShape from
     * @return {@link ShapeBuilder} read from the parser or null
     *          if the parsers current token has been <code>null</code>
     * @throws IOException if the input could not be read
     */
    static ShapeBuilder<?, ?, ?> parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    static ShapeBuilder<?, ?, ?> parse(Object value) throws IOException {
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap("value", value), null)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parse(parser);
        }
    }
}
