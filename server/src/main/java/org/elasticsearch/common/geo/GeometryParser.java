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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

/**
 * An utility class with a geometry parser methods supporting different shape representation formats
 */
public final class GeometryParser {

    private final GeoJson geoJsonParser;
    private final WellKnownText wellKnownTextParser;

    public GeometryParser(boolean rightOrientation, boolean coerce, boolean ignoreZValue) {
        geoJsonParser = new GeoJson(rightOrientation, coerce, ignoreZValue);
        wellKnownTextParser = new WellKnownText(coerce, ignoreZValue);
    }

    /**
     * Parses supplied XContent into Geometry
     */
    public Geometry parse(XContentParser parser) throws IOException,
        ParseException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return geoJsonParser.fromXContent(parser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            // TODO: Add support for ignoreZValue and coerce to WKT
            return wellKnownTextParser.fromWKT(parser.text());
        }
        throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
    }
}
