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
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An utility class with a geometry parser methods supporting different shape representation formats
 */
public final class GeometryParser {

    private final GeoJson geoJsonParser;
    private final WellKnownText wellKnownTextParser;
    private final boolean ignoreZValue;

    public GeometryParser(boolean rightOrientation, boolean coerce, boolean ignoreZValue) {
        GeometryValidator validator = new StandardValidator(ignoreZValue);
        geoJsonParser = new GeoJson(rightOrientation, coerce, validator);
        wellKnownTextParser = new WellKnownText(coerce, validator);
        this.ignoreZValue = ignoreZValue;
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
    public GeometryFormat<Geometry> geometryFormat(String format) {
        if (format.equals(GeoJsonGeometryFormat.NAME)) {
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else if (format.equals(WKTGeometryFormat.NAME)) {
            return new WKTGeometryFormat(wellKnownTextParser);
        } else {
            throw new IllegalArgumentException("Unrecognized geometry format [" + format + "].");
        }
    }

    /**
     * Returns a geometry format object that can parse and then serialize the object back to the same format.
     * This method automatically recognizes the format by examining the provided {@link XContentParser}.
     */
    public GeometryFormat<Geometry> geometryFormat(XContentParser parser) {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new WKTGeometryFormat(wellKnownTextParser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            // We don't know the format of the original geometry - so going with default
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else {
            throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
        }
    }

    /**
     * Parses the value as a {@link Geometry}. The following types of values are supported:
     * <p>
     * Object: has to contain either lat and lon or geohash fields
     * <p>
     * String: expected to be in "latitude, longitude" format, a geohash or WKT
     * <p>
     * Array: two or more elements, the first element is longitude, the second is latitude, the rest is ignored if ignoreZValue is true
     * <p>
     * Json structure: valid geojson definition
     */
    public  Geometry parseGeometry(Object value) throws ElasticsearchParseException {
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            if (values.size() == 2 && values.get(0) instanceof Number) {
                GeoPoint point = GeoUtils.parseGeoPoint(values, ignoreZValue);
                return new Point(point.lon(), point.lat());
            } else {
                List<Geometry> geometries = new ArrayList<>(values.size());
                for (Object object : values) {
                    geometries.add(parseGeometry(object));
                }
                return new GeometryCollection<>(geometries);
            }
        }
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            Collections.singletonMap("null_value", value), null)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            if (isPoint(value)) {
                GeoPoint point = GeoUtils.parseGeoPoint(parser, new GeoPoint(), ignoreZValue);
                return new Point(point.lon(), point.lat());
            } else {
                return parse(parser);
            }

        } catch (IOException | ParseException ex) {
            throw new ElasticsearchParseException("error parsing geometry ", ex);
        }
    }

    private boolean isPoint(Object value) {
        // can we do this better?
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            return map.containsKey("lat") && map.containsKey("lon");
        } else if (value instanceof String) {
            String string = (String) value;
            return Character.isDigit(string.charAt(0)) || string.indexOf('(') == -1;
        }
        return false;
    }
}
