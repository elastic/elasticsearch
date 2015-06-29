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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxQuery;
import org.elasticsearch.index.search.geo.IndexedGeoBoundingBoxQuery;

import java.io.IOException;
import java.util.Objects;

/**
 * Creates a Lucene query that will filter for all documents that lie within the specified
 * bounding box.
 *
 * This query can only operate on fields of type geo_point that have latitude and longitude
 * enabled.
 * */
public class GeoBoundingBoxQueryBuilder extends AbstractQueryBuilder<GeoBoundingBoxQueryBuilder> {
    /** Name of the query. */
    public static final String NAME = "geo_bbox";
    /** Default for geo point coerce (as of this writing false). */
    public static final boolean DEFAULT_COERCE = false;
    /** Default for skipping geo point validation (as of this writing false). */
    public static final boolean DEFAULT_IGNORE_MALFORMED = false;
    /** Default type for executing this query (memory as of this writing). */
    public static final Type DEFAULT_TYPE = Type.MEMORY;
    /** Needed for serialization. */
    static final GeoBoundingBoxQueryBuilder PROTOTYPE = new GeoBoundingBoxQueryBuilder();

    /** Name of field holding geo coordinates to compute the bounding box on.*/
    private final String fieldName;
    /** Top left corner coordinates of bounding box. */
    private GeoPoint topLeft = new GeoPoint(Double.NaN, Double.NaN);
    /** Bottom right corner coordinates of bounding box.*/
    private GeoPoint bottomRight = new GeoPoint(Double.NaN, Double.NaN);
    /** Whether or not to infer correct coordinates for wrapping bounding boxes.*/
    private boolean coerce = DEFAULT_COERCE;
    /** Whether or not to skip geo point validation. */
    private boolean ignoreMalformed = DEFAULT_IGNORE_MALFORMED;
    /** How the query should be run. */
    private Type type = DEFAULT_TYPE;

    /**
     * For serialization only.
     * */
    private GeoBoundingBoxQueryBuilder() {
        this.fieldName = null;
    }

    /**
     * Create new bounding box query.
     * @param fieldName name of index field containing geo coordinates to operate on.
     * */
    public GeoBoundingBoxQueryBuilder(String fieldName) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("Field name must not be empty.");
        }
        this.fieldName = fieldName;
    }

    /**
     * Adds top left point.
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder topLeft(double lat, double lon) {
        topLeft.reset(lat, lon);
        return this;
    }

    /**
     * Adds top left point.
     * @param point point to add.
     * */
    public GeoBoundingBoxQueryBuilder topLeft(GeoPoint point) {
        return topLeft(point.getLat(), point.getLon());
    }

    /**
     * Adds top left point
     * @param geohash GeoHash to derive the point from.
     * */
    public GeoBoundingBoxQueryBuilder topLeft(String geohash) {
        return topLeft(GeoHashUtils.decode(geohash));
    }

    /** Returns the top left corner of the bounding box. */
    public GeoPoint topLeft() {
        return topLeft;
    }

    /**
     * Adds bottom right corner.
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder bottomRight(double lat, double lon) {
        this.bottomRight.reset(lat, lon);
        return this;
    }

    /**
     * Adds bottom right corner.
     * @param point the corner coordinates.
     */
    public GeoBoundingBoxQueryBuilder bottomRight(GeoPoint point) {
        return bottomRight(point.getLat(), point.getLon());
    }

    /**
     * Adds bottom right corner.
     * @param geohash the corner coordinates.
     */
    public GeoBoundingBoxQueryBuilder bottomRight(String geohash) {
        return bottomRight(GeoHashUtils.decode(geohash));
    }

    /** Returns the bottom right corner of the bounding box. */
    public GeoPoint bottomRight() {
        return bottomRight;
    }
    /**
     * Adds bottom left corner.
     *
     * @param lat The latitude
     * @param lon The longitude
     */
    public GeoBoundingBoxQueryBuilder bottomLeft(double lat, double lon) {
        bottomRight.resetLat(lat);
        topLeft.resetLon(lon);
        return this;
    }

    /**
     * Adds bottom left corner.
     *
     * @param point bottom left corner of bounding box.
     */
    public GeoBoundingBoxQueryBuilder bottomLeft(GeoPoint point) {
        return bottomLeft(point.lat(), point.lon());
    }

    /**
     * Adds bottom left corner.
     *
     * @param point bottom left corner of bounding box.
     */
    public GeoBoundingBoxQueryBuilder bottomLeft(String geohash) {
        return bottomLeft(GeoHashUtils.decode(geohash));
    }

    /**
     * Adds top right corner.
     *
     * @param lat latitude of top right corner.
     * @param lon longitude of top right corner.
     */
    public GeoBoundingBoxQueryBuilder topRight(double lat, double lon) {
        topLeft.resetLat(lat);
        bottomRight.resetLon(lon);
        return this;
    }

    /**
     * Adds top right corner.
     *
     * @param point topRight corner of bounding box.
     */
    public GeoBoundingBoxQueryBuilder topRight(GeoPoint point) {
        return topRight(point.lat(), point.lon());
    }

    /**
     * Adds top right corner.
     *
     * @param geohash topRight corner of bounding box.
     */
    public GeoBoundingBoxQueryBuilder topRight(String geohash) {
        return topRight(GeoHashUtils.decode(geohash));
    }

    /**
     * Specify whether or not to try and fix broken/wrapping bounding boxes.
     * If set to true, also enables ignoreMalformed thus disabling geo point
     * validation altogether.
     **/
    public GeoBoundingBoxQueryBuilder coerce(boolean coerce) {
        if (coerce) {
            this.ignoreMalformed = true;
        }
        this.coerce = coerce;
        return this;
    }

    /** Returns whether or not to try and fix broken/wrapping bounding boxes. */
    public boolean coerce() {
        return this.coerce;
    }

    /**
     * Specify whether or not to ignore validation errors of bounding boxes.
     * Can only be set if coerce set to false, otherwise calling this
     * method has no effect.
     **/
    public GeoBoundingBoxQueryBuilder ignoreMalformed(boolean ignoreMalformed) {
        if (coerce == false) {
            this.ignoreMalformed = ignoreMalformed;
        }
        return this;
    }

    /** Returns whether or not to skip bounding box validation. */
    public boolean ignoreMalformed() {
        return ignoreMalformed;
    }

    /**
     * Sets the type of executing of the geo bounding box. Can be either `memory` or `indexed`. Defaults
     * to `memory`.
     */
    public GeoBoundingBoxQueryBuilder type(Type type) {
        this.type = (type != null) ? type : DEFAULT_TYPE;
        return this;
    }

    /**
     * For BWC: Parse type from type name.
     * */
    public GeoBoundingBoxQueryBuilder type(String type) {
        this.type = Type.fromString(type);
        return this;
    }
    /** Returns the execution type of the geo bounding box.*/
    public Type type() {
        return type;
    }

    /** Returns the name of the field to base the bounding box computation on. */
    public String fieldName() {
        return this.fieldName;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (!ignoreMalformed) {
            if (Numbers.isValidDouble(topLeft.getLat()) == false) {
                validationException = addValidationError("top latitude is invalid number: " + topLeft.getLat(), validationException);
            }
            if (Numbers.isValidDouble(topLeft.getLon()) == false) {
                validationException = addValidationError("left longitude is invalid number: " + topLeft.getLon(), validationException);
            }
            if (Numbers.isValidDouble(bottomRight.getLat()) == false) {
                validationException = addValidationError("bottom latitude is invalid number: " + bottomRight.getLat(), validationException);
            }
            if (Numbers.isValidDouble(bottomRight.getLon()) == false) {
                validationException = addValidationError("right longitude is invalid number: " + bottomRight.getLon(), validationException);
            }

            if (validationException == null) {
                // all corners are valid after above checks - make sure they are in the right relation
                if (topLeft.getLat() < bottomRight.getLat()) {
                    validationException = addValidationError("top is below bottom corner: " +
                            topLeft.getLat() + " vs. " + bottomRight.getLat(), validationException);
                }

                // we do not check longitudes as the query generation code can deal with flipped left/right values
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
        if (GeoUtils.isValidLatitude(topLeft.getLat()) == false) {
            validationException = addValidationError("top latitude is invalid: " + topLeft.getLat(),
                    validationException);
        }
        if (GeoUtils.isValidLongitude(topLeft.getLon()) == false) {
            validationException = addValidationError("left longitude is invalid: " + topLeft.getLon(),
                    validationException);
        }
        if (GeoUtils.isValidLatitude(bottomRight.getLat()) == false) {
            validationException = addValidationError("bottom latitude is invalid: " + bottomRight.getLat(),
                    validationException);
        }
        if (GeoUtils.isValidLongitude(bottomRight.getLon()) == false) {
            validationException = addValidationError("right longitude is invalid: " + bottomRight.getLon(),
                    validationException);
        }
        return validationException;
    }

    @Override
    public Query doToQuery(QueryShardContext context) {
        QueryValidationException exception = validate(context.indexVersionCreated().before(Version.V_2_0_0));
        if (exception != null) {
            throw new QueryShardException(context, "couldn't validate latitude/ longitude values", exception);
        }

        GeoPoint luceneTopLeft = new GeoPoint(topLeft);
        GeoPoint luceneBottomRight = new GeoPoint(bottomRight);
        if (coerce) {
            // Special case: if the difference between the left and right is 360 and the right is greater than the left, we are asking for
            // the complete longitude range so need to set longitude to the complete longditude range
            double right = luceneBottomRight.getLon();
            double left = luceneTopLeft.getLon();

            boolean completeLonRange = ((right - left) % 360 == 0 && right > left);
            GeoUtils.normalizePoint(luceneTopLeft, true, !completeLonRange);
            GeoUtils.normalizePoint(luceneBottomRight, true, !completeLonRange);
            if (completeLonRange) {
                luceneTopLeft.resetLon(-180);
                luceneBottomRight.resetLon(180);
            }
        }

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }
        GeoPointFieldMapper.GeoPointFieldType geoFieldType = ((GeoPointFieldMapper.GeoPointFieldType) fieldType);

        Query result;
        switch(type) {
            case INDEXED:
                result = IndexedGeoBoundingBoxQuery.create(luceneTopLeft, luceneBottomRight, geoFieldType);
                break;
            case MEMORY:
                IndexGeoPointFieldData indexFieldData = context.getForField(fieldType);
                result = new InMemoryGeoBoundingBoxQuery(luceneTopLeft, luceneBottomRight, indexFieldData);
                break;
            default:
                // Someone extended the type enum w/o adjusting this switch statement.
                throw new QueryShardException(context, "geo bounding box type [" + type
                    + "] not supported.");
        }

        return result;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.array(GeoBoundingBoxQueryParser.TOP_LEFT, topLeft.getLon(), topLeft.getLat());
        builder.array(GeoBoundingBoxQueryParser.BOTTOM_RIGHT, bottomRight.getLon(), bottomRight.getLat());
        builder.endObject();
        builder.field("coerce", coerce);
        builder.field("ignore_malformed", ignoreMalformed);
        builder.field("type", type);

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    @Override
    public boolean doEquals(GeoBoundingBoxQueryBuilder other) {
        return Objects.equals(topLeft, other.topLeft) &&
                Objects.equals(bottomRight, other.bottomRight) &&
                Objects.equals(type, other.type) &&
                Objects.equals(coerce, other.coerce) &&
                Objects.equals(ignoreMalformed, other.ignoreMalformed) &&
                Objects.equals(fieldName, other.fieldName);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(topLeft, bottomRight, type, coerce, ignoreMalformed, fieldName);
    }

    @Override
    public GeoBoundingBoxQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        GeoBoundingBoxQueryBuilder geo = new GeoBoundingBoxQueryBuilder(fieldName);
        geo.topLeft = geo.topLeft.readFrom(in);
        geo.bottomRight = geo.bottomRight.readFrom(in);
        geo.type = Type.readTypeFrom(in);
        geo.coerce = in.readBoolean();
        geo.ignoreMalformed = in.readBoolean();
        return geo;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        topLeft.writeTo(out);
        bottomRight.writeTo(out);
        type.writeTo(out);
        out.writeBoolean(coerce);
        out.writeBoolean(ignoreMalformed);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /** Specifies how the bounding box query should be run. */
    public enum Type implements Writeable<Type> {
        MEMORY(0), INDEXED(1);

        private final int ordinal;

        private static final Type PROTOTYPE = MEMORY;

        Type(int ordinal) {
            this.ordinal = ordinal;
        }

        @Override
        public Type readFrom(StreamInput in) throws IOException {
            int ord = in.readVInt();
            switch(ord) {
                case(0): return MEMORY;
                case(1): return INDEXED;
            }
            throw new ElasticsearchException("unknown serialized type [" + ord + "]");
        }

        public static Type readTypeFrom(StreamInput in) throws IOException {
            return PROTOTYPE.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }

        public static Type fromString(String typeName) {
            if (typeName == null) {
                throw new IllegalArgumentException("cannot parse type from null string");
            }

            for (Type type : Type.values()) {
                if (type.name().equalsIgnoreCase(typeName)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("no type can be parsed from ordinal " + typeName);
        }
    }
}
