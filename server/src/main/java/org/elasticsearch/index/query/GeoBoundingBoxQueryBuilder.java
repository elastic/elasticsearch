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

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
//import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.parsers.GeoWKTParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;

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
    public static final String NAME = "geo_bounding_box";

    /** Default type for executing this query (memory as of this writing). */
    public static final GeoExecType DEFAULT_TYPE = GeoExecType.MEMORY;

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    private static final ParseField TOP_FIELD = new ParseField("top");
    private static final ParseField BOTTOM_FIELD = new ParseField("bottom");
    private static final ParseField LEFT_FIELD = new ParseField("left");
    private static final ParseField RIGHT_FIELD = new ParseField("right");
    private static final ParseField TOP_LEFT_FIELD = new ParseField("top_left");
    private static final ParseField BOTTOM_RIGHT_FIELD = new ParseField("bottom_right");
    private static final ParseField TOP_RIGHT_FIELD = new ParseField("top_right");
    private static final ParseField BOTTOM_LEFT_FIELD = new ParseField("bottom_left");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");
    private static final ParseField WKT_FIELD = new ParseField("wkt");


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

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

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
     * Read from a stream.
     */
    public GeoBoundingBoxQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        topLeft = in.readGeoPoint();
        bottomRight = in.readGeoPoint();
        type = GeoExecType.readFromStream(in);
        validationMethod = GeoValidationMethod.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGeoPoint(topLeft);
        out.writeGeoPoint(bottomRight);
        type.writeTo(out);
        validationMethod.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
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
            } else if (top == bottom) {
                throw new IllegalArgumentException("top cannot be the same as bottom: " +
                    top + " == " + bottom);
            } else if (left == right) {
                throw new IllegalArgumentException("left cannot be the same as right: " +
                    left + " == " + right);
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
     * Adds points from a single geohash.
     * @param geohash The geohash for computing the bounding box.
     */
    public GeoBoundingBoxQueryBuilder setCorners(final String geohash) {
        // get the bounding box of the geohash and set topLeft and bottomRight
        Rectangle ghBBox = Geohash.toBoundingBox(geohash);
        return setCorners(new GeoPoint(ghBBox.getMaxY(), ghBBox.getMinX()), new GeoPoint(ghBBox.getMinY(), ghBBox.getMaxX()));
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

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoBoundingBoxQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
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

    QueryValidationException checkLatLon() {
        if (GeoValidationMethod.isIgnoreMalformed(validationMethod)) {
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
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
            }
        }
        if (!(fieldType instanceof GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        QueryValidationException exception = checkLatLon();
        if (exception != null) {
            throw new QueryShardException(context, "couldn't validate latitude/ longitude values", exception);
        }

        GeoPoint luceneTopLeft = new GeoPoint(topLeft);
        GeoPoint luceneBottomRight = new GeoPoint(bottomRight);
        if (GeoValidationMethod.isCoerce(validationMethod)) {
            // Special case: if the difference between the left and right is 360 and the right is greater than the left, we are asking for
            // the complete longitude range so need to set longitude to the complete longitude range
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

        Query query = LatLonPoint.newBoxQuery(fieldType.name(), luceneBottomRight.getLat(), luceneTopLeft.getLat(),
            luceneTopLeft.getLon(), luceneBottomRight.getLon());
        if (fieldType.hasDocValues()) {
            Query dvQuery = LatLonDocValuesField.newSlowBoxQuery(fieldType.name(),
                    luceneBottomRight.getLat(), luceneTopLeft.getLat(),
                    luceneTopLeft.getLon(), luceneBottomRight.getLon());
            query = new IndexOrDocValuesQuery(query, dvQuery);
        }
        return query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.array(TOP_LEFT_FIELD.getPreferredName(), topLeft.getLon(), topLeft.getLat());
        builder.array(BOTTOM_RIGHT_FIELD.getPreferredName(), bottomRight.getLon(), bottomRight.getLat());
        builder.endObject();
        builder.field(VALIDATION_METHOD_FIELD.getPreferredName(), validationMethod);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    public static GeoBoundingBoxQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        GeoValidationMethod validationMethod = null;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        // bottom (minLat), top (maxLat), left (minLon), right (maxLon)
        double[] bbox = null;
        String type = "memory";

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                try {
                    bbox = parseBoundingBox(parser);
                    fieldName = currentFieldName;
                } catch (Exception e) {
                    throw new ElasticsearchParseException("failed to parse [{}] query. [{}]", NAME, e.getMessage());
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (VALIDATION_METHOD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    type = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "failed to parse [{}] query. unexpected field [{}]",
                            NAME, currentFieldName);
                }
            }
        }

        if (bbox == null) {
            throw new ElasticsearchParseException("failed to parse [{}] query. bounding box not provided", NAME);
        }

        final GeoPoint topLeft = new GeoPoint(bbox[1], bbox[2]);
        final GeoPoint bottomRight = new GeoPoint(bbox[0], bbox[3]);

        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        builder.setCorners(topLeft, bottomRight);
        builder.queryName(queryName);
        builder.boost(boost);
        builder.type(GeoExecType.fromString(type));
        builder.ignoreUnmapped(ignoreUnmapped);
        if (validationMethod != null) {
            // ignore deprecated coerce/ignoreMalformed settings if validationMethod is set
            builder.setValidationMethod(validationMethod);
        }
        return builder;
    }

    @Override
    protected boolean doEquals(GeoBoundingBoxQueryBuilder other) {
        return Objects.equals(topLeft, other.topLeft) &&
                Objects.equals(bottomRight, other.bottomRight) &&
                Objects.equals(type, other.type) &&
                Objects.equals(validationMethod, other.validationMethod) &&
                Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(topLeft, bottomRight, type, validationMethod, fieldName, ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Parses the bounding box and returns bottom, top, left, right coordinates
     */
    public static double[] parseBoundingBox(XContentParser parser) throws IOException, ElasticsearchParseException {
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("failed to parse bounding box. Expected start object but found [{}]", token);
        }

        double top = Double.NaN;
        double bottom = Double.NaN;
        double left = Double.NaN;
        double right = Double.NaN;

        String currentFieldName;
        GeoPoint sparse = new GeoPoint();
        EnvelopeBuilder envelope = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                token = parser.nextToken();
                if (WKT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    envelope = (EnvelopeBuilder)(GeoWKTParser.parseExpectedType(parser, GeoShapeType.ENVELOPE));
                } else if (TOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    top = parser.doubleValue();
                } else if (BOTTOM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    bottom = parser.doubleValue();
                } else if (LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    left = parser.doubleValue();
                } else if (RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    right = parser.doubleValue();
                } else {
                    if (TOP_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.TOP_LEFT);
                        top = sparse.getLat();
                        left = sparse.getLon();
                    } else if (BOTTOM_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.BOTTOM_RIGHT);
                        bottom = sparse.getLat();
                        right = sparse.getLon();
                    } else if (TOP_RIGHT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.TOP_RIGHT);
                        top = sparse.getLat();
                        right = sparse.getLon();
                    } else if (BOTTOM_LEFT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        GeoUtils.parseGeoPoint(parser, sparse, false, GeoUtils.EffectivePoint.BOTTOM_LEFT);
                        bottom = sparse.getLat();
                        left = sparse.getLon();
                    } else {
                        throw new ElasticsearchParseException("failed to parse bounding box. unexpected field [{}]", currentFieldName);
                    }
                }
            } else {
                throw new ElasticsearchParseException("failed to parse bounding box. field name expected but [{}] found", token);
            }
        }
        if (envelope != null) {
            if (Double.isNaN(top) == false || Double.isNaN(bottom) == false || Double.isNaN(left) == false ||
                Double.isNaN(right) == false) {
                throw new ElasticsearchParseException("failed to parse bounding box. Conflicting definition found "
                    + "using well-known text and explicit corners.");
            }
            org.locationtech.spatial4j.shape.Rectangle r = envelope.buildS4J();
            return new double[]{r.getMinY(), r.getMaxY(), r.getMinX(), r.getMaxX()};
        }
        return new double[]{bottom, top, left, right};
    }
}
