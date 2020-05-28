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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

/**
 * An utility class with a geometry parser methods supporting different shape representation formats
 */
public final class GeometryParser {

    private final GeoJson geoJsonParser;
    private final WellKnownText wellKnownTextParser;

    public GeometryParser(boolean rightOrientation, boolean coerce, boolean ignoreZValue) {
        GeometryValidator validator = new StandardValidator(ignoreZValue);
        geoJsonParser = new GeoJson(rightOrientation, coerce, validator);
        wellKnownTextParser = new WellKnownText(coerce, validator);
    }

    /**
     * Parses supplied XContent into Geometry
     */
    public Geometry parse(XContentParser parser) throws IOException, ParseException {
        return geometryFormat(parser).fromXContent(parser);
    }

    /**
     * Returns a geometry format object that can parse and then serialize the object back to the same format.
     */
    public GeometryFormat<Geometry> geometryFormat(XContentParser parser) {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return new GeometryFormat<Geometry>() {
                @Override
                public Geometry fromXContent(XContentParser parser) throws IOException {
                    return null;
                }

                @Override
                public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
                    if (geometry != null) {
                        // We don't know the format of the original geometry - so going with default
                        return GeoJson.toXContent(geometry, builder, params);
                    } else {
                        return builder.nullValue();
                    }
                }
            };
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new GeometryFormat<Geometry>() {
                @Override
                public Geometry fromXContent(XContentParser parser) throws IOException {
                    return geoJsonParser.fromXContent(parser);
                }

                @Override
                public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
                    if (geometry != null) {
                        return GeoJson.toXContent(geometry, builder, params);
                    } else {
                        return builder.nullValue();
                    }
                }
            };
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new GeometryFormat<Geometry>() {
                @Override
                public Geometry fromXContent(XContentParser parser) throws IOException, ParseException {
                    return wellKnownTextParser.fromWKT(parser.text());
                }

                @Override
                public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
                    if (geometry != null) {
                        return builder.value(wellKnownTextParser.toWKT(geometry));
                    } else {
                        return builder.nullValue();
                    }
                }
            };

        }
        throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
    }
}
