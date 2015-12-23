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

import org.apache.lucene.search.GeoPointInBBoxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapperLegacy;
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
    /** Default type for executing this query (memory as of this writing). */
    public static final GeoExecType DEFAULT_TYPE = GeoExecType.MEMORY;
    /** Needed for serialization. */
    static final GeoBoundingBoxQueryBuilder PROTOTYPE = new GeoBoundingBoxQueryBuilder("");

    /** Name of field holding geo coordinates to compute the bounding box on.*/
    private final String fieldName;
    /** Top left corner coordinates of bounding box. */
    private GeoPoint topLeft = new GeoPoint(Double.NaN, Double.NaN);
    /** Bottom right corner coordinates of bounding box.*/
    private GeoPoint bottomRight = new GeoPoint(Double.NaN, Double.NaN);
    /** How to deal with incorrect coordinates.*/
    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;
    /** How the query should be run. */
    private GeoExecType type = DEFAULT_TYPE;

    /**
     * Create new bounding box query.
     * @param fieldName name of index field containing geo coordinates to operate on.
     * */
    public GeoBoundingBoxQueryBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("Field name must not be empty.");
        }
        this.fieldName = fieldName;
    }

    /**
     * Adds top left point.
     * @param top The top latitude
     * @param left The left longitude
     * @param bottom The bottom latitude
     * @param right The right longitude
     */
    public GeoBoundingBoxQueryBuilder setCorners(double top, double left, double bottom, double right) {
        if (GeoValidationMethod.isIgnoreMalformed(validationMethod) == false) {
            if (Numbers.isValidDouble(top) == false) {
                throw new IllegalArgumentException("top latitude is invalid: " + top);
            }
            if (Numbers.isValidDouble(left) == false) {
                throw new IllegalArgumentException("left longitude is invalid: " + left);
            }
            if (Numbers.isValidDouble(bottom) == false) {
                throw new IllegalArgumentException("bottom latitude is invalid: " + bottom);
            }
            if (Numbers.isValidDouble(right) == false) {
                throw new IllegalArgumentException("right longitude is invalid: " + right);
            }

            // all corners are valid after above checks - make sure they are in the right relation
            if (top < bottom) {
                throw new IllegalArgumentException("top is below bottom corner: " +
                            top + " vs. " + bottom);
                }

                // we do not check longitudes as the query generation code can deal with flipped left/right values
        }
        
        topLeft.reset(top, left);
        bottomRight.reset(bottom, right);
        return this;
    }

    /**
     * Adds points.
     * @param topLeft topLeft point to add.
     * @param bottomRight bottomRight point to add.
     * */
    public GeoBoundingBoxQueryBuilder setCorners(GeoPoint topLeft, GeoPoint bottomRight) {
        return setCorners(topLeft.getLat(), topLeft.getLon(), bottomRight.getLat(), bottomRight.getLon());
    }

    /**
     * Adds points.
     * @param topLeft topLeft point to add as geohash.
     * @param bottomRight bottomRight point to add as geohash.
     * */
    public GeoBoundingBoxQueryBuilder setCorners(String topLeft, String bottomRight) {
        return setCorners(GeoPoint.fromGeohash(topLeft), GeoPoint.fromGeohash(bottomRight));
    }

    /** Returns the top left corner of the bounding box. */
    public GeoPoint topLeft() {
        return topLeft;
    }
    
    /** Returns the bottom right corner of the bounding box. */
    public GeoPoint bottomRight() {
        return bottomRight;
    }

    /**
     * Adds corners in OGC standard bbox/ envelop format.
     *
     * @param bottomLeft bottom left corner of bounding box.
     * @param topRight top right corner of bounding box.
     */
    public GeoBoundingBoxQueryBuilder setCornersOGC(GeoPoint bottomLeft, GeoPoint topRight) {
        return setCorners(topRight.getLat(), bottomLeft.getLon(), bottomLeft.getLat(), topRight.getLon());
    }

    /**
     * Adds corners in OGC standard bbox/ envelop format.
     *
     * @param bottomLeft bottom left corner geohash.
     * @param topRight top right corner geohash.
     */
    public GeoBoundingBoxQueryBuilder setCornersOGC(String bottomLeft, String topRight) {
        return setCornersOGC(GeoPoint.fromGeohash(bottomLeft), GeoPoint.fromGeohash(topRight));
    }

    /**
     * Specify whether or not to ignore validation errors of bounding boxes.
     * Can only be set if coerce set to false, otherwise calling this
     * method has no effect.
     **/
    public GeoBoundingBoxQueryBuilder setValidationMethod(GeoValidationMethod method) {
        this.validationMethod = method;
        return this;
    }
    
    /**
     * Returns geo coordinate validation method to use.
     * */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
    }

    /**
     * Sets the type of executing of the geo bounding box. Can be either `memory` or `indexed`. Defaults
     * to `memory`.
     */
    public GeoBoundingBoxQueryBuilder type(GeoExecType type) {
        if (type == null) {
            throw new IllegalArgumentException("Type is not allowed to be null.");
        }
        this.type = type;
        return this;
    }

    /**
     * For BWC: Parse type from type name.
     * */
    public GeoBoundingBoxQueryBuilder type(String type) {
        this.type = GeoExecType.fromString(type);
        return this;
    }
    /** Returns the execution type of the geo bounding box.*/
    public GeoExecType type() {
        return type;
    }

    /** Returns the name of the field to base the bounding box computation on. */
    public String fieldName() {
        return this.fieldName;
    }

    QueryValidationException checkLatLon(boolean indexCreatedBeforeV2_0) {
        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        if (GeoValidationMethod.isIgnoreMalformed(validationMethod) == true || indexCreatedBeforeV2_0) {
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
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        QueryValidationException exception = checkLatLon(context.indexVersionCreated().before(Version.V_2_0_0));
        if (exception != null) {
            throw new QueryShardException(context, "couldn't validate latitude/ longitude values", exception);
        }

        GeoPoint luceneTopLeft = new GeoPoint(topLeft);
        GeoPoint luceneBottomRight = new GeoPoint(bottomRight);
        if (GeoValidationMethod.isCoerce(validationMethod)) {
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

        if (context.indexVersionCreated().onOrAfter(Version.V_2_2_0)) {
            return new GeoPointInBBoxQuery(fieldType.name(), luceneTopLeft.lon(), luceneBottomRight.lat(),
                    luceneBottomRight.lon(), luceneTopLeft.lat());
        }

        Query query;
        switch(type) {
            case INDEXED:
                GeoPointFieldMapperLegacy.GeoPointFieldType geoFieldType = ((GeoPointFieldMapperLegacy.GeoPointFieldType) fieldType);
                query = IndexedGeoBoundingBoxQuery.create(luceneTopLeft, luceneBottomRight, geoFieldType);
                break;
            case MEMORY:
                IndexGeoPointFieldData indexFieldData = context.getForField(fieldType);
                query = new InMemoryGeoBoundingBoxQuery(luceneTopLeft, luceneBottomRight, indexFieldData);
                break;
            default:
                // Someone extended the type enum w/o adjusting this switch statement.
                throw new IllegalStateException("geo bounding box type [" + type + "] not supported.");
        }

        return query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.array(GeoBoundingBoxQueryParser.TOP_LEFT_FIELD.getPreferredName(), topLeft.getLon(), topLeft.getLat());
        builder.array(GeoBoundingBoxQueryParser.BOTTOM_RIGHT_FIELD.getPreferredName(), bottomRight.getLon(), bottomRight.getLat());
        builder.endObject();
        builder.field(GeoBoundingBoxQueryParser.VALIDATION_METHOD_FIELD.getPreferredName(), validationMethod);
        builder.field(GeoBoundingBoxQueryParser.TYPE_FIELD.getPreferredName(), type);

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    @Override
    protected boolean doEquals(GeoBoundingBoxQueryBuilder other) {
        return Objects.equals(topLeft, other.topLeft) &&
                Objects.equals(bottomRight, other.bottomRight) &&
                Objects.equals(type, other.type) &&
                Objects.equals(validationMethod, other.validationMethod) &&
                Objects.equals(fieldName, other.fieldName);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(topLeft, bottomRight, type, validationMethod, fieldName);
    }

    @Override
    protected GeoBoundingBoxQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        GeoBoundingBoxQueryBuilder geo = new GeoBoundingBoxQueryBuilder(fieldName);
        geo.topLeft = in.readGeoPoint();
        geo.bottomRight = in.readGeoPoint();
        geo.type = GeoExecType.readTypeFrom(in);
        geo.validationMethod = GeoValidationMethod.readGeoValidationMethodFrom(in);
        return geo;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGeoPoint(topLeft);
        out.writeGeoPoint(bottomRight);
        type.writeTo(out);
        validationMethod.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
