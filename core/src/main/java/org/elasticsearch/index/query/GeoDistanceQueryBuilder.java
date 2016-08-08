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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.GeoPointDistanceQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapperLegacy;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Filter results of a query to include only those within a specific distance to some
 * geo point.
 */
public class GeoDistanceQueryBuilder extends AbstractQueryBuilder<GeoDistanceQueryBuilder> {
    public static final String NAME = "geo_distance";

    /** Default for latitude normalization (as of this writing true).*/
    public static final boolean DEFAULT_NORMALIZE_LAT = true;
    /** Default for longitude normalization (as of this writing true). */
    public static final boolean DEFAULT_NORMALIZE_LON = true;
    /** Default for distance unit computation. */
    public static final DistanceUnit DEFAULT_DISTANCE_UNIT = DistanceUnit.DEFAULT;
    /** Default for geo distance computation. */
    public static final GeoDistance DEFAULT_GEO_DISTANCE = GeoDistance.DEFAULT;
    /** Default for optimising query through pre computed bounding box query. */
    public static final String DEFAULT_OPTIMIZE_BBOX = "memory";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    private static final ParseField IGNORE_MALFORMED_FIELD = new ParseField("ignore_malformed")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField COERCE_FIELD = new ParseField("coerce", "normalize")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField OPTIMIZE_BBOX_FIELD = new ParseField("optimize_bbox");
    private static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");
    private static final ParseField UNIT_FIELD = new ParseField("unit");
    private static final ParseField DISTANCE_FIELD = new ParseField("distance");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String fieldName;
    /** Distance from center to cover. */
    private double distance;
    /** Point to use as center. */
    private GeoPoint center = new GeoPoint(Double.NaN, Double.NaN);
    /** Algorithm to use for distance computation. */
    private GeoDistance geoDistance = DEFAULT_GEO_DISTANCE;
    /** Whether or not to use a bbox for pre-filtering. TODO change to enum? */
    private String optimizeBbox = DEFAULT_OPTIMIZE_BBOX;
    /** How strict should geo coordinate validation be? */
    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    /**
     * Construct new GeoDistanceQueryBuilder.
     * @param fieldName name of indexed geo field to operate distance computation on.
     * */
    public GeoDistanceQueryBuilder(String fieldName) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("fieldName must not be null or empty");
        }
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public GeoDistanceQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        distance = in.readDouble();
        validationMethod = GeoValidationMethod.readFromStream(in);
        center = in.readGeoPoint();
        optimizeBbox = in.readString();
        geoDistance = GeoDistance.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeDouble(distance);
        validationMethod.writeTo(out);
        out.writeGeoPoint(center);
        out.writeString(optimizeBbox);
        geoDistance.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
    }

    /** Name of the field this query is operating on. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Sets the center point for the query.
     * @param point the center of the query
     **/
    public GeoDistanceQueryBuilder point(GeoPoint point) {
        if (point == null) {
            throw new IllegalArgumentException("center point must not be null");
        }
        this.center = point;
        return this;
    }

    /**
     * Sets the center point of the query.
     * @param lat latitude of center
     * @param lon longitude of center
     * */
    public GeoDistanceQueryBuilder point(double lat, double lon) {
        this.center = new GeoPoint(lat, lon);
        return this;
    }

    /** Returns the center point of the distance query. */
    public GeoPoint point() {
        return this.center;
    }

    /** Sets the distance from the center using the default distance unit.*/
    public GeoDistanceQueryBuilder distance(String distance) {
        return distance(distance, DistanceUnit.DEFAULT);
    }

    /** Sets the distance from the center for this query. */
    public GeoDistanceQueryBuilder distance(String distance, DistanceUnit unit) {
        if (Strings.isEmpty(distance)) {
            throw new IllegalArgumentException("distance must not be null or empty");
        }
        if (unit == null) {
            throw new IllegalArgumentException("distance unit must not be null");
        }
        double newDistance = DistanceUnit.parse(distance, unit, DistanceUnit.DEFAULT);
        if (newDistance <= 0.0) {
            throw new IllegalArgumentException("distance must be greater than zero");
        }
        this.distance = newDistance;
        return this;
    }

    /** Sets the distance from the center for this query. */
    public GeoDistanceQueryBuilder distance(double distance, DistanceUnit unit) {
        return distance(Double.toString(distance), unit);
    }

    /** Returns the distance configured as radius. */
    public double distance() {
        return distance;
    }

    /** Sets the center point for this query. */
    public GeoDistanceQueryBuilder geohash(String geohash) {
        if (Strings.isEmpty(geohash)) {
            throw new IllegalArgumentException("geohash must not be null or empty");
        }
        this.center.resetFromGeoHash(geohash);
        return this;
    }

    /** Which type of geo distance calculation method to use. */
    public GeoDistanceQueryBuilder geoDistance(GeoDistance geoDistance) {
        if (geoDistance == null) {
            throw new IllegalArgumentException("geoDistance must not be null");
        }
        this.geoDistance = geoDistance;
        return this;
    }

    /** Returns geo distance calculation type to use. */
    public GeoDistance geoDistance() {
        return this.geoDistance;
    }

    /**
     * Set this to memory or indexed if before running the distance
     * calculation you want to limit the candidates to hits in the
     * enclosing bounding box.
     **/
    public GeoDistanceQueryBuilder optimizeBbox(String optimizeBbox) {
        if (optimizeBbox == null) {
            throw new IllegalArgumentException("optimizeBbox must not be null");
        }
        switch (optimizeBbox) {
            case "none":
            case "memory":
            case "indexed":
                break;
            default:
                throw new IllegalArgumentException("optimizeBbox must be one of [none, memory, indexed]");
        }
        this.optimizeBbox = optimizeBbox;
        return this;
    }

    /**
     * Returns whether or not to run a BoundingBox query prior to
     * distance query for optimization purposes.*/
    public String optimizeBbox() {
        return this.optimizeBbox;
    }

    /** Set validation method for geo coordinates. */
    public void setValidationMethod(GeoValidationMethod method) {
        this.validationMethod = method;
    }

    /** Returns validation method for geo coordinates. */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
    }

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoDistanceQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
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
    protected Query doToQuery(QueryShardContext shardContext) throws IOException {
        MappedFieldType fieldType = shardContext.fieldMapper(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(shardContext, "failed to find geo_point field [" + fieldName + "]");
            }
        }

        if (!(fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(shardContext, "field [" + fieldName + "] is not a geo_point field");
        }

        final Version indexVersionCreated = shardContext.indexVersionCreated();
        QueryValidationException exception = checkLatLon(shardContext.indexVersionCreated().before(Version.V_2_0_0));
        if (exception != null) {
            throw new QueryShardException(shardContext, "couldn't validate latitude/ longitude values", exception);
        }

        if (indexVersionCreated.onOrAfter(Version.V_2_2_0) || GeoValidationMethod.isCoerce(validationMethod)) {
            GeoUtils.normalizePoint(center, true, true);
        }

        double normDistance = geoDistance.normalize(this.distance, DistanceUnit.DEFAULT);

        if (indexVersionCreated.before(Version.V_2_2_0)) {
            GeoPointFieldMapperLegacy.GeoPointFieldType geoFieldType = ((GeoPointFieldMapperLegacy.GeoPointFieldType) fieldType);
            IndexGeoPointFieldData indexFieldData = shardContext.getForField(fieldType);
            return new GeoDistanceRangeQuery(center, null, normDistance, true, false, geoDistance,
                    geoFieldType, indexFieldData, optimizeBbox);
        }

        // if index created V_2_2 use (soon to be legacy) numeric encoding postings format
        // if index created V_2_3 > use prefix encoded postings format
        final GeoPointField.TermEncoding encoding = (indexVersionCreated.before(Version.V_2_3_0)) ?
            GeoPointField.TermEncoding.NUMERIC : GeoPointField.TermEncoding.PREFIX;
        // Lucene 6.0 and earlier requires a radial restriction
        if (indexVersionCreated.before(Version.V_5_0_0_alpha4)) {
            normDistance = GeoUtils.maxRadialDistance(center, normDistance);
        }
        return new GeoPointDistanceQuery(fieldType.name(), encoding, center.lat(), center.lon(), normDistance);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(fieldName).value(center.lon()).value(center.lat()).endArray();
        builder.field(DISTANCE_FIELD.getPreferredName(), distance);
        builder.field(DISTANCE_TYPE_FIELD.getPreferredName(), geoDistance.name().toLowerCase(Locale.ROOT));
        builder.field(OPTIMIZE_BBOX_FIELD.getPreferredName(), optimizeBbox);
        builder.field(VALIDATION_METHOD_FIELD.getPreferredName(), validationMethod);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<GeoDistanceQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        GeoPoint point = new GeoPoint(Double.NaN, Double.NaN);
        String fieldName = null;
        Object vDistance = null;
        DistanceUnit unit = GeoDistanceQueryBuilder.DEFAULT_DISTANCE_UNIT;
        GeoDistance geoDistance = GeoDistanceQueryBuilder.DEFAULT_GEO_DISTANCE;
        String optimizeBbox = GeoDistanceQueryBuilder.DEFAULT_OPTIMIZE_BBOX;
        boolean coerce = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        boolean ignoreMalformed = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        GeoValidationMethod validationMethod = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_ARRAY) {
                fieldName = currentFieldName;
                GeoUtils.parseGeoPoint(parser, point);
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "[geo_distance] query doesn't support multiple fields, found ["
                            + fieldName + "] and [" + currentFieldName + "]");
                }
                // the json in the format of -> field : { lat : 30, lon : 12 }
                String currentName = parser.currentName();
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentName = parser.currentName();
                    } else if (token.isValue()) {
                        if (currentName.equals(GeoPointFieldMapper.Names.LAT)) {
                            point.resetLat(parser.doubleValue());
                        } else if (currentName.equals(GeoPointFieldMapper.Names.LON)) {
                            point.resetLon(parser.doubleValue());
                        } else if (currentName.equals(GeoPointFieldMapper.Names.GEOHASH)) {
                            point.resetFromGeoHash(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[geo_distance] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, DISTANCE_FIELD)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        vDistance = parser.text(); // a String
                    } else {
                        vDistance = parser.numberValue(); // a Number
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, UNIT_FIELD)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, DISTANCE_TYPE_FIELD)) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LAT_SUFFIX)) {
                    point.resetLat(parser.doubleValue());
                    fieldName = currentFieldName.substring(0, currentFieldName.length() - GeoPointFieldMapper.Names.LAT_SUFFIX.length());
                } else if (currentFieldName.endsWith(GeoPointFieldMapper.Names.LON_SUFFIX)) {
                    point.resetLon(parser.doubleValue());
                    fieldName = currentFieldName.substring(0, currentFieldName.length() - GeoPointFieldMapper.Names.LON_SUFFIX.length());
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, OPTIMIZE_BBOX_FIELD)) {
                    optimizeBbox = parser.textOrNull();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, COERCE_FIELD)) {
                    coerce = parser.booleanValue();
                    if (coerce == true) {
                        ignoreMalformed = true;
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_MALFORMED_FIELD)) {
                    ignoreMalformed = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_UNMAPPED_FIELD)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, VALIDATION_METHOD_FIELD)) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else {
                    if (fieldName == null) {
                        point.resetFromString(parser.text());
                        fieldName = currentFieldName;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[" + GeoDistanceQueryBuilder.NAME +
                                "] field name already set to [" + fieldName + "] but found [" + currentFieldName + "]");
                    }
                }
            }
        }

        if (vDistance == null) {
            throw new ParsingException(parser.getTokenLocation(), "geo_distance requires 'distance' to be specified");
        }

        GeoDistanceQueryBuilder qb = new GeoDistanceQueryBuilder(fieldName);
        if (vDistance instanceof Number) {
            qb.distance(((Number) vDistance).doubleValue(), unit);
        } else {
            qb.distance((String) vDistance, unit);
        }
        qb.point(point);
        if (validationMethod != null) {
            qb.setValidationMethod(validationMethod);
        } else {
            qb.setValidationMethod(GeoValidationMethod.infer(coerce, ignoreMalformed));
        }
        qb.optimizeBbox(optimizeBbox);
        qb.geoDistance(geoDistance);
        qb.boost(boost);
        qb.queryName(queryName);
        qb.ignoreUnmapped(ignoreUnmapped);
        return Optional.of(qb);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(center, geoDistance, optimizeBbox, distance, validationMethod, ignoreUnmapped);
    }

    @Override
    protected boolean doEquals(GeoDistanceQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                (distance == other.distance) &&
                Objects.equals(validationMethod, other.validationMethod) &&
                Objects.equals(center, other.center) &&
                Objects.equals(optimizeBbox, other.optimizeBbox) &&
                Objects.equals(geoDistance, other.geoDistance) &&
                Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    private QueryValidationException checkLatLon(boolean indexCreatedBeforeV2_0) {
        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (GeoValidationMethod.isIgnoreMalformed(validationMethod) || indexCreatedBeforeV2_0) {
            return null;
        }

        QueryValidationException validationException = null;
        // For everything post 2.0, validate latitude and longitude unless validation was explicitly turned off
        if (GeoUtils.isValidLatitude(center.getLat()) == false) {
            validationException = addValidationError("center point latitude is invalid: " + center.getLat(), validationException);
        }
        if (GeoUtils.isValidLongitude(center.getLon()) == false) {
            validationException = addValidationError("center point longitude is invalid: " + center.getLon(), validationException);
        }
        return validationException;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
