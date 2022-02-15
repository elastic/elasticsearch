/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    /** Name of field holding geo coordinates to compute the bounding box on.*/
    private final String fieldName;
    private GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN));
    /** How to deal with incorrect coordinates.*/
    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

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
        geoBoundingBox = new GeoBoundingBox(in);
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readVInt(); // ignore value
        }
        validationMethod = GeoValidationMethod.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        geoBoundingBox.writeTo(out);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeVInt(0);
        }
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
                throw new IllegalArgumentException("top is below bottom corner: " + top + " vs. " + bottom);
            } else if (top == bottom) {
                throw new IllegalArgumentException("top cannot be the same as bottom: " + top + " == " + bottom);
            } else if (left == right) {
                throw new IllegalArgumentException("left cannot be the same as right: " + left + " == " + right);
            }

            // we do not check longitudes as the query generation code can deal with flipped left/right values
        }

        geoBoundingBox.topLeft().reset(top, left);
        geoBoundingBox.bottomRight().reset(bottom, right);
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
        return geoBoundingBox.topLeft();
    }

    /** Returns the bottom right corner of the bounding box. */
    public GeoPoint bottomRight() {
        return geoBoundingBox.bottomRight();
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

        GeoPoint topLeft = geoBoundingBox.topLeft();
        GeoPoint bottomRight = geoBoundingBox.bottomRight();

        QueryValidationException validationException = null;
        // For everything post 2.0 validate latitude and longitude unless validation was explicitly turned off
        if (GeoUtils.isValidLatitude(topLeft.getLat()) == false) {
            validationException = addValidationError("top latitude is invalid: " + topLeft.getLat(), validationException);
        }
        if (GeoUtils.isValidLongitude(topLeft.getLon()) == false) {
            validationException = addValidationError("left longitude is invalid: " + topLeft.getLon(), validationException);
        }
        if (GeoUtils.isValidLatitude(bottomRight.getLat()) == false) {
            validationException = addValidationError("bottom latitude is invalid: " + bottomRight.getLat(), validationException);
        }
        if (GeoUtils.isValidLongitude(bottomRight.getLon()) == false) {
            validationException = addValidationError("right longitude is invalid: " + bottomRight.getLon(), validationException);
        }
        return validationException;
    }

    @Override
    public Query doToQuery(SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo field [" + fieldName + "]");
            }
        }
        if ((fieldType instanceof GeoShapeQueryable) == false) {
            throw new QueryShardException(
                context,
                "Field [" + fieldName + "] is of unsupported type [" + fieldType.typeName() + "] for [" + NAME + "] query"
            );
        }

        QueryValidationException exception = checkLatLon();
        if (exception != null) {
            throw new QueryShardException(context, "couldn't validate latitude/ longitude values", exception);
        }

        GeoPoint luceneTopLeft = new GeoPoint(geoBoundingBox.topLeft());
        GeoPoint luceneBottomRight = new GeoPoint(geoBoundingBox.bottomRight());
        if (GeoValidationMethod.isCoerce(validationMethod)) {
            // Special case: if the difference between the left and right is 360 and the right is greater than the left, we are asking for
            // the complete longitude range so need to set longitude to the complete longitude range
            double right = luceneBottomRight.getLon();
            double left = luceneTopLeft.getLon();

            boolean completeLonRange = ((right - left) % 360 == 0 && right > left);
            GeoUtils.normalizePoint(luceneTopLeft, true, completeLonRange == false);
            GeoUtils.normalizePoint(luceneBottomRight, true, completeLonRange == false);
            if (completeLonRange) {
                luceneTopLeft.resetLon(-180);
                luceneBottomRight.resetLon(180);
            }
        }

        final GeoShapeQueryable geoShapeQueryable = (GeoShapeQueryable) fieldType;
        final Rectangle rectangle = new Rectangle(
            luceneTopLeft.getLon(),
            luceneBottomRight.getLon(),
            luceneTopLeft.getLat(),
            luceneBottomRight.getLat()
        );
        return geoShapeQueryable.geoShapeQuery(rectangle, fieldType.name(), SpatialStrategy.RECURSIVE, ShapeRelation.INTERSECTS, context);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        geoBoundingBox.toXContentFragment(builder, false);
        builder.endObject();
        builder.field(VALIDATION_METHOD_FIELD.getPreferredName(), validationMethod);
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
        GeoBoundingBox bbox = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                try {
                    bbox = GeoBoundingBox.parseBoundingBox(parser);
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
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "failed to parse [{}] query. unexpected field [{}]",
                        NAME,
                        currentFieldName
                    );
                }
            }
        }

        if (bbox == null) {
            throw new ElasticsearchParseException("failed to parse [{}] query. bounding box not provided", NAME);
        }

        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        builder.setCorners(bbox.topLeft(), bbox.bottomRight());
        builder.queryName(queryName);
        builder.boost(boost);
        builder.ignoreUnmapped(ignoreUnmapped);
        if (validationMethod != null) {
            // ignore deprecated coerce/ignoreMalformed settings if validationMethod is set
            builder.setValidationMethod(validationMethod);
        }
        return builder;
    }

    @Override
    protected boolean doEquals(GeoBoundingBoxQueryBuilder other) {
        return Objects.equals(geoBoundingBox, other.geoBoundingBox)
            && Objects.equals(validationMethod, other.validationMethod)
            && Objects.equals(fieldName, other.fieldName)
            && Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(geoBoundingBox, validationMethod, fieldName, ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }
}
