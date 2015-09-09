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

import com.google.common.base.Preconditions;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Numbers;
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
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
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
    /** Default for distance unit computation. */
    public static final DistanceUnit DEFAULT_DISTANCE_UNIT = DistanceUnit.DEFAULT;
    /** Default for geo distance computation. */
    public static final GeoDistance DEFAULT_GEO_DISTANCE = GeoDistance.DEFAULT;
    /** Default for optimising query through pre computed bounding box query. */
    public static final String DEFAULT_OPTIMIZE_BBOX = "memory";

    public static final boolean DEFAULT_COERCE = false;
    public static final boolean DEFAULT_IGNORE_MALFORMED = false;

    private final String fieldName;
    /** Distance from center to cover. */
    private double distance;
    /** Point to use as center. */
    private GeoPoint center = new GeoPoint(Double.NaN, Double.NaN);
    /** Algorithm to use for distance computation. */
    private GeoDistance geoDistance = DEFAULT_GEO_DISTANCE;
    /** Whether or not to use a bbox for pre-filtering. TODO change to enum? */
    private String optimizeBbox = DEFAULT_OPTIMIZE_BBOX;

    private boolean coerce = DEFAULT_COERCE;

    private boolean ignoreMalformed = DEFAULT_IGNORE_MALFORMED;


    static final GeoDistanceQueryBuilder PROTOTYPE = new GeoDistanceQueryBuilder();

    /** For serialization purposes only. */
    private GeoDistanceQueryBuilder() {
        this.fieldName = null;
    }

    /** Name of the field this query is operating on. */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * Construct new GeoDistanceQueryBuilder.
     * @param fieldName name of indexed geo field to operate distance computation on.
     * */
    public GeoDistanceQueryBuilder(String name) {
        if (Strings.isEmpty(name)) {
            throw new IllegalArgumentException("Fieldname must not be null or empty");
        }
        this.fieldName = name;
    }

    /** Sets the center point for the query. 
     * @param point the center of the query
     **/
    public GeoDistanceQueryBuilder point(GeoPoint point) {
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

    /** Sets the latitude of the center. */
    public GeoDistanceQueryBuilder lat(double lat) {
        if (this.center == null) {
            this.center = new GeoPoint(lat, Double.NaN);
        } else {
            this.center.resetLat(lat);
        }
        return this;
    }

    /** Sets the longitude of the center. */
    public GeoDistanceQueryBuilder lon(double lon) {
        if (this.center == null) {
            this.center = new GeoPoint(Double.NaN, lon);
        } else {
            this.center.resetLon(lon);
        }
        return this;
    }
//
    /** Returns the center point of the distance query. */
    public GeoPoint point() {
        return this.center;
    }

    /** Sets the distance from the center using the default distance unit.*/
    public GeoDistanceQueryBuilder distance(String distance) {
        return this.distance(distance, DistanceUnit.DEFAULT);
    }

    /** Sets the distance from the center for this query. */
    public GeoDistanceQueryBuilder distance(String distance, DistanceUnit unit) {
        this.distance = DistanceUnit.parse(distance, unit, DistanceUnit.DEFAULT);
        return this;
    }

    /** Sets the distance from the center for this query. */
    public GeoDistanceQueryBuilder distance(double distance, DistanceUnit unit) {
        this.distance = DistanceUnit.DEFAULT.convert(distance, unit);
        return this;
    }

    /** Returns the distance configured as radius. */
    public double distance() {
        return distance;
    }

    /** Sets the center point for this query. */
    public GeoDistanceQueryBuilder geohash(String geohash) {
        this.center.resetFromGeoHash(geohash);
        return this;
    }

    /** Which type of geo distance calculation method to use. */
    public GeoDistanceQueryBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = (geoDistance != null) ? geoDistance : DEFAULT_GEO_DISTANCE;
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
        this.optimizeBbox = (optimizeBbox != null) ? optimizeBbox : DEFAULT_OPTIMIZE_BBOX;
        return this;
    }

    /**
     * Returns whether or not to run a BoundingBox query prior to
     * distance query for optimization purposes.*/
    public String optimizeBbox() {
        return this.optimizeBbox;
    }

    public GeoDistanceQueryBuilder coerce(boolean coerce) {
        this.coerce = coerce;
        if (this.coerce) {
            this.ignoreMalformed = true;
        }
        return this;
    }
    
    public boolean coerce() {
        return this.coerce;
    }

    public GeoDistanceQueryBuilder ignoreMalformed(boolean ignoreMalformed) {
        if (this.coerce == false) {
            this.ignoreMalformed = ignoreMalformed;
        }
        return this;
    }
    
    public boolean ignoreMalformed() {
        return this.ignoreMalformed;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        QueryValidationException exception = validate(context.indexVersionCreated().before(Version.V_2_0_0));
        if (exception != null) {
            throw new QueryShardException(context, "couldn't validate latitude/ longitude values", exception);
        }

        double normDistance = geoDistance.normalize(this.distance, DistanceUnit.DEFAULT);

        if (coerce) {
            GeoUtils.normalizePoint(center, true, true);
        }

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }
        GeoPointFieldMapper.GeoPointFieldType geoFieldType = ((GeoPointFieldMapper.GeoPointFieldType) fieldType);

        IndexGeoPointFieldData indexFieldData = context.getForField(fieldType);
        Query query = new GeoDistanceRangeQuery(center, null, normDistance, true, false, geoDistance, geoFieldType, indexFieldData, optimizeBbox);
        return query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray(fieldName).value(center.lon()).value(center.lat()).endArray();
        builder.field("distance", distance);
        if (geoDistance != null) {
            builder.field("distance_type", geoDistance.name().toLowerCase(Locale.ROOT));
        }
        if (optimizeBbox != null) {
            builder.field("optimize_bbox", optimizeBbox);
        }
        builder.field("coerce", coerce);
        builder.field("ignore_malformed", ignoreMalformed);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public int doHashCode() {
        return Objects.hash(center, geoDistance, optimizeBbox, distance, coerce, ignoreMalformed);
    }

    @Override
    public boolean doEquals(GeoDistanceQueryBuilder other) {
        return Objects.equals(this.fieldName, other.fieldName) &&
                (this.distance == other.distance) &&
                (this.coerce == other.coerce) &&
                (this.ignoreMalformed == other.ignoreMalformed) &&
                Objects.equals(center, other.center) &&
                Objects.equals(optimizeBbox, other.optimizeBbox) &&
                Objects.equals(geoDistance, other.geoDistance);
    }

    @Override
    protected GeoDistanceQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        GeoDistanceQueryBuilder result = new GeoDistanceQueryBuilder(fieldName);
        result.distance = in.readDouble();
        result.coerce = in.readBoolean();
        result.ignoreMalformed = in.readBoolean();
        double lat = in.readDouble();
        double lon = in.readDouble();
        result.point(lat, lon);
        result.optimizeBbox = in.readString();
        result.geoDistance = GeoDistance.readGeoDistanceFrom(in);
        return result;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeDouble(distance);
        out.writeBoolean(coerce);
        out.writeBoolean(ignoreMalformed);
        out.writeDouble(center.getLat());
        out.writeDouble(center.getLon());
        out.writeString(optimizeBbox);
        geoDistance.writeTo(out);
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (!ignoreMalformed) {
            if (Numbers.isValidDouble(center.getLat()) == false) {
                validationException = addValidationError("top latitude is invalid number: " + center.getLat(), validationException);
            }
            if (Numbers.isValidDouble(center.getLon()) == false) {
                validationException = addValidationError("left longitude is invalid number: " + center.getLon(), validationException);
            }
        }

        if (Strings.isEmpty(fieldName)) {
            validationException = addValidationError("field name must be non-null and non-empty", validationException);
        }
        return validationException;
    }

    QueryValidationException validate(boolean indexCreatedBeforeV2_0) {
        if (ignoreMalformed || indexCreatedBeforeV2_0) {
            return null;
        }

        QueryValidationException validationException = null;
        // For everything post 2.0 validate latitude and longitude unless validation was explicitly turned off
        if (GeoUtils.isValidLatitude(center.getLat()) == false) {
            validationException = addValidationError("top latitude is invalid: " + center.getLat(),
                    validationException);
        }
        if (GeoUtils.isValidLongitude(center.getLon()) == false) {
            validationException = addValidationError("left longitude is invalid: " + center.getLon(),
                    validationException);
        }
        return validationException;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
