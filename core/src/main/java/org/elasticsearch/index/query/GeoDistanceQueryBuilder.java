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

import org.apache.lucene.search.GeoPointDistanceQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapperLegacy;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Filter results of a query to include only those within a specific distance to some
 * geo point.
 * */
public class GeoDistanceQueryBuilder extends AbstractQueryBuilder<GeoDistanceQueryBuilder> {

    /** Name of the query in the query dsl. */
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

    static final GeoDistanceQueryBuilder PROTOTYPE = new GeoDistanceQueryBuilder("_na_");

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

    /** Set validaton method for geo coordinates. */
    public void setValidationMethod(GeoValidationMethod method) {
        this.validationMethod = method;
    }

    /** Returns validation method for geo coordinates. */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
    }

    @Override
    protected Query doToQuery(QueryShardContext shardContext) throws IOException {
        MappedFieldType fieldType = shardContext.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(shardContext, "failed to find geo_point field [" + fieldName + "]");
        }

        if (!(fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(shardContext, "field [" + fieldName + "] is not a geo_point field");
        }

        QueryValidationException exception = checkLatLon(shardContext.indexVersionCreated().before(Version.V_2_0_0));
        if (exception != null) {
            throw new QueryShardException(shardContext, "couldn't validate latitude/ longitude values", exception);
        }

        if (GeoValidationMethod.isCoerce(validationMethod)) {
            GeoUtils.normalizePoint(center, true, true);
        }

        double normDistance = geoDistance.normalize(this.distance, DistanceUnit.DEFAULT);

        if (shardContext.indexVersionCreated().before(Version.V_2_2_0)) {
            GeoPointFieldMapperLegacy.GeoPointFieldType geoFieldType = ((GeoPointFieldMapperLegacy.GeoPointFieldType) fieldType);
            IndexGeoPointFieldData indexFieldData = shardContext.getForField(fieldType);
            return new GeoDistanceRangeQuery(center, null, normDistance, true, false, geoDistance, geoFieldType, indexFieldData, optimizeBbox);
        }

        normDistance = GeoUtils.maxRadialDistance(center, normDistance);
        return new GeoPointDistanceQuery(fieldType.name(), center.lon(), center.lat(), normDistance);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(fieldName).value(center.lon()).value(center.lat()).endArray();
        builder.field(GeoDistanceQueryParser.DISTANCE_FIELD.getPreferredName(), distance);
        builder.field(GeoDistanceQueryParser.DISTANCE_TYPE_FIELD.getPreferredName(), geoDistance.name().toLowerCase(Locale.ROOT));
        builder.field(GeoDistanceQueryParser.OPTIMIZE_BBOX_FIELD.getPreferredName(), optimizeBbox);
        builder.field(GeoDistanceQueryParser.VALIDATION_METHOD_FIELD.getPreferredName(), validationMethod);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(center, geoDistance, optimizeBbox, distance, validationMethod);
    }

    @Override
    protected boolean doEquals(GeoDistanceQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                (distance == other.distance) &&
                Objects.equals(validationMethod, other.validationMethod) &&
                Objects.equals(center, other.center) &&
                Objects.equals(optimizeBbox, other.optimizeBbox) &&
                Objects.equals(geoDistance, other.geoDistance);
    }

    @Override
    protected GeoDistanceQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        GeoDistanceQueryBuilder result = new GeoDistanceQueryBuilder(fieldName);
        result.distance = in.readDouble();
        result.validationMethod = GeoValidationMethod.readGeoValidationMethodFrom(in);
        result.center = in.readGeoPoint();
        result.optimizeBbox = in.readString();
        result.geoDistance = GeoDistance.readGeoDistanceFrom(in);
        return result;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeDouble(distance);
        validationMethod.writeTo(out);
        out.writeGeoPoint(center);
        out.writeString(optimizeBbox);
        geoDistance.writeTo(out);
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
