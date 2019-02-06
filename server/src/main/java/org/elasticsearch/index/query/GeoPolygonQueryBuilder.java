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

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GeoPolygonQueryBuilder extends AbstractQueryBuilder<GeoPolygonQueryBuilder> {
    public static final String NAME = "geo_polygon";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;
    private static final ParseField VALIDATION_METHOD = new ParseField("validation_method");
    private static final ParseField POINTS_FIELD = new ParseField("points");
    private static final ParseField MULTIPOLYGON_FIELD = new ParseField("multipolygon");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String fieldName;

    private final List<List<List<GeoPoint>>> polygons;

    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    public GeoPolygonQueryBuilder(String fieldName, List multiPoly) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        if (multiPoly == null || multiPoly.size() == 0) {
            throw new IllegalArgumentException("polygon must not be null or empty");
        }
        this.fieldName = fieldName;
        if (multiPoly.get(0) instanceof List) {
            // handle multipolygon queries
            this.polygons = new ArrayList<>(multiPoly);
            // seal the boundaries
            for (int p=0; p<polygons.size(); ++p) {
                List polygon = polygons.get(p);
                for (int b=0; b<polygon.size(); ++b) {
                    closeBoundary((List<GeoPoint>)polygon.get(b));
                }
            }
        } else if (multiPoly.get(0) instanceof GeoPoint) {
            // handles { points : [ ... for backward compatibility
            List<GeoPoint> shell = new ArrayList<>(multiPoly);
            closeBoundary(shell);
            List<List<GeoPoint>> poly = new ArrayList<>();
            poly.add(shell);
            this.polygons = new ArrayList<>();
            this.polygons.add(poly);
        } else {
            throw new IllegalArgumentException("invalid multipolygon found");
        }
    }

    /** Read from a stream. */
    public GeoPolygonQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        int numPolys = in.readVInt();
        this.polygons = new ArrayList<>(numPolys);
        for (int p=0; p<numPolys; ++p) {
            int numBoundaries = in.readVInt();
            List<List<GeoPoint>> boundaries = new ArrayList<>(numBoundaries);
            for (int b=0; b<numBoundaries; ++b) {
                int numPoints = in.readVInt();
                List<GeoPoint> points = new ArrayList<>(numPoints);
                for (int i=0; i<numPoints; ++i) {
                    points.add(in.readGeoPoint());
                }
                boundaries.add(points);
            }
            this.polygons.add(boundaries);
        }
        validationMethod = GeoValidationMethod.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    /** Write to a stream. */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeVInt(polygons.size());
        for (List<List<GeoPoint>> polygon : polygons) {
            out.writeVInt(polygon.size());
            for (List<GeoPoint> boundary : polygon) {
                out.writeVInt(boundary.size());
                for (GeoPoint point : boundary) {
                    out.writeGeoPoint(point);
                }
            }
        }
        validationMethod.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
    }

    public String fieldName() {
        return fieldName;
    }

    public List<List<List<GeoPoint>>> points() {
        return polygons;
    }

    /** Sets the validation method to use for geo coordinates. */
    public GeoPolygonQueryBuilder setValidationMethod(GeoValidationMethod method) {
        this.validationMethod = method;
        return this;
    }

    /** Returns the validation method to use for geo coordinates. */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
    }

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoPolygonQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
            }
        }
        if (!(fieldType instanceof GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        List<List<List<GeoPoint>>> polys = new ArrayList<>(this.polygons);
        Polygon[] multiPoly = new Polygon[polys.size()];
        int p = 0;
        for (List<List<GeoPoint>> poly : polys) {
            assert poly.size() > 0;
            Polygon[] holes = new Polygon[poly.size() - 1];
            double[] shellLat = null;
            double[] shellLon = null;
            for (int b=0; b<poly.size(); ++b) {
                List<GeoPoint> boundary = poly.get(b);
                int l = 0;
                double[] lat = new double[boundary.size()];
                double[] lon = new double[boundary.size()];
                for (GeoPoint point : boundary) {
                    // if validation method is set to coerce or ignoreMalformed; normalize the point
                    if (GeoValidationMethod.isCoerce(validationMethod)
                        || GeoValidationMethod.isIgnoreMalformed(validationMethod)) {
                        GeoUtils.normalizePoint(point, true, true);
                    }
                    lat[l] = point.lat();
                    lon[l++] = point.lon();
                }
                if (b == 0) {
                    shellLat = lat;
                    shellLon = lon;
                } else {
                    holes[b-1] = new Polygon(lat, lon);
                }
            }
            multiPoly[p++] = new Polygon(shellLat, shellLon, holes);
        }

        return LatLonPoint.newPolygonQuery(fieldType.name(), multiPoly);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.startArray(MULTIPOLYGON_FIELD.getPreferredName());
        for (int p=0; p<polygons.size(); ++p) {
            List<List<GeoPoint>> polygon = polygons.get(p);
            builder.startArray();
            for (int b=0; b<polygon.size(); ++b) {
                List<GeoPoint> points = polygon.get(b);
                builder.startArray();
                for (GeoPoint point : points) {
                    builder.startArray().value(point.lon()).value(point.lat()).endArray();
                }
                builder.endArray();
            }
            builder.endArray();
        }
        builder.endArray();
        builder.endObject();

        builder.field(VALIDATION_METHOD.getPreferredName(), validationMethod);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    protected static List<GeoPoint> parseBoundary(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<GeoPoint> boundary = new ArrayList<>();
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (token == Token.START_ARRAY) {
                boundary.add(GeoUtils.parseGeoPoint(parser, new GeoPoint()));
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "[geo_polygon] query does not support token type [" + token.name() + "]");
            }
        }

        return boundary;
    }

    protected static List<List<GeoPoint>> parsePolygon(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<List<GeoPoint>> boundaries = new ArrayList<>();
        // loop through boundaries
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (token == Token.START_ARRAY) {
                boundaries.add(parseBoundary(parser));
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "[geo_polygon] query does not support token type [" + token.name() + "]");
            }
        }
        return boundaries;
    }

    protected static List<List<List<GeoPoint>>> parseMultiPolygon(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<List<List<GeoPoint>>> polygons = new ArrayList<>();
        // loop through all polygons
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (token == Token.START_ARRAY) {
               polygons.add(parsePolygon(parser));
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "[geo_polygon] query does not support token type [" + token.name() + "]");
            }
        }
        return polygons;
    }

    public static GeoPolygonQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;

        List shell = null;

        Float boost = null;
        GeoValidationMethod validationMethod = null;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (POINTS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            shell = new ArrayList<>();
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                shell.add(GeoUtils.parseGeoPoint(parser));
                            }
                        } else if (MULTIPOLYGON_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            // parse a multipolygon as defined in the GeoJSON spec
                            //   Note: shape validation is not performed, possible todo but perhaps overkill
                            shell = parseMultiPolygon(parser);
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[geo_polygon] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[geo_polygon] query does not support token type [" + token.name() + "] under [" + currentFieldName + "]");
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (VALIDATION_METHOD.match(currentFieldName, parser.getDeprecationHandler())) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[geo_polygon] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[geo_polygon] unexpected token type [" + token.name() + "]");
            }
        }
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(fieldName, shell);
        if (validationMethod != null) {
            // if GeoValidationMethod was explicitly set ignore deprecated coerce and ignoreMalformed settings
            builder.setValidationMethod(validationMethod);
        }

        if (queryName != null) {
            builder.queryName(queryName);
        }
        if (boost != null) {
            builder.boost(boost);
        }
        builder.ignoreUnmapped(ignoreUnmapped);
        return builder;
    }

    @Override
    protected boolean doEquals(GeoPolygonQueryBuilder other) {
        return Objects.equals(validationMethod, other.validationMethod)
                && Objects.equals(fieldName, other.fieldName)
                && Objects.equals(polygons, other.polygons)
                && Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(validationMethod, fieldName, polygons, ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private void closeBoundary(List<GeoPoint> boundary) {
        if (!boundary.get(boundary.size() - 1).equals(boundary.get(0))) {
            boundary.add(boundary.get(0));
        }
        if (boundary.size() < 4) {
            throw new IllegalArgumentException("too few points defined for geo_polygon query");
        }
    }
}
