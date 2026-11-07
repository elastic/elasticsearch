/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Supported formats to read/write JSON geometries.
 */
public enum GeometryParserFormat {

    WKT {
        @Override
        public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
            if (geometry != null) {
                return builder.value(WellKnownText.toWKT(geometry));
            } else {
                return builder.nullValue();
            }
        }

        @Override
        public Geometry fromXContent(GeometryValidator validator, boolean coerce, boolean rightOrientation, XContentParser parser)
            throws IOException, ParseException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            if (parser instanceof MapXContentParser) {
                // we have already serialized the string so using optimize text will allocate a new byte array
                return WellKnownText.fromWKT(validator, coerce, parser.text());
            }
            XContentString.UTF8Bytes utfBytes = parser.optimizedText().bytes();
            return WellKnownText.fromWKT(validator, coerce, utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
        }

    },
    GEOJSON {
        @Override
        public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
            if (geometry != null) {
                return GeoJson.toXContent(geometry, builder, params);
            } else {
                return builder.nullValue();
            }
        }

        @Override
        public Geometry fromXContent(GeometryValidator validator, boolean coerce, boolean rightOrientation, XContentParser parser)
            throws IOException, ParseException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            return GeoJson.fromXContent(validator, coerce, rightOrientation, parser);
        }
    };

    /**
     * Serializes the geometry into its JSON representation
     */
    public abstract XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException;

    /**
     * Parser JSON representation of a geometry
     */
    public abstract Geometry fromXContent(GeometryValidator validator, boolean coerce, boolean rightOrientation, XContentParser parser)
        throws IOException, ParseException;

    /**
     * Returns a geometry parser format object that can parse and then serialize the object back to the same format.
     * This method automatically recognizes the format by examining the provided {@link XContentParser}.
     */
    public static GeometryParserFormat geometryFormat(XContentParser parser) {
        return switch (parser.currentToken()) {
            // We don't know the format of the original geometry - so going with default
            case START_OBJECT, VALUE_NULL -> GEOJSON;
            case VALUE_STRING -> WKT;
            default -> throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
        };
    }
}
