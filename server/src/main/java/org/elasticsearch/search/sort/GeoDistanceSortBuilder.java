/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.DoubleComparator;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.AbstractLatLonPointIndexFieldData.LatLonPointIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.search.sort.FieldSortBuilder.validateMaxChildrenExistOnlyInTopLevelNestedSort;
import static org.elasticsearch.search.sort.FieldSortBuilder.validateMissingNestedPath;
import static org.elasticsearch.search.sort.NestedSortBuilder.NESTED_FIELD;

/**
 * A geo distance based sorting on a geo point like field.
 */
public class GeoDistanceSortBuilder extends SortBuilder<GeoDistanceSortBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GeoDistanceSortBuilder.class);

    public static final String NAME = "_geo_distance";
    public static final String ALTERNATIVE_NAME = "_geoDistance";
    public static final GeoValidationMethod DEFAULT_VALIDATION = GeoValidationMethod.DEFAULT;

    private static final ParseField UNIT_FIELD = new ParseField("unit");
    private static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");
    private static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    private static final ParseField SORTMODE_FIELD = new ParseField("mode", "sort_mode");
    private static final ParseField IGNORE_UNMAPPED = new ParseField("ignore_unmapped");

    private final String fieldName;
    private final List<GeoPoint> points = new ArrayList<>();

    private GeoDistance geoDistance = GeoDistance.ARC;
    private DistanceUnit unit = DistanceUnit.DEFAULT;

    private SortMode sortMode = null;

    private NestedSortBuilder nestedSort;

    private GeoValidationMethod validation = DEFAULT_VALIDATION;

    private boolean ignoreUnmapped = false;

    /**
     * Constructs a new distance based sort on a geo point like field.
     *
     * @param fieldName The geo point like field name.
     * @param points The points to create the range distance facets from.
     */
    public GeoDistanceSortBuilder(String fieldName, GeoPoint... points) {
        this.fieldName = fieldName;
        if (points.length == 0) {
            throw new IllegalArgumentException("Geo distance sorting needs at least one point.");
        }
        this.points.addAll(Arrays.asList(points));
    }

    /**
     * Constructs a new distance based sort on a geo point like field.
     *
     * @param fieldName The geo point like field name.
     * @param lat Latitude of the point to create the range distance facets from.
     * @param lon Longitude of the point to create the range distance facets from.
     */
    public GeoDistanceSortBuilder(String fieldName, double lat, double lon) {
        this(fieldName, new GeoPoint(lat, lon));
    }

    /**
     * Constructs a new distance based sort on a geo point like field.
     *
     * @param fieldName The geo point like field name.
     * @param geohashes The points to create the range distance facets from.
     */
    public GeoDistanceSortBuilder(String fieldName, String... geohashes) {
        if (geohashes.length == 0) {
            throw new IllegalArgumentException("Geo distance sorting needs at least one point.");
        }
        for (String geohash : geohashes) {
            this.points.add(GeoPoint.fromGeohash(geohash));
        }
        this.fieldName = fieldName;
    }

    /**
     * Copy constructor.
     * */
    GeoDistanceSortBuilder(GeoDistanceSortBuilder original) {
        this.fieldName = original.fieldName();
        this.points.addAll(original.points);
        this.geoDistance = original.geoDistance;
        this.unit = original.unit;
        this.order = original.order;
        this.sortMode = original.sortMode;
        this.validation = original.validation;
        this.nestedSort = original.nestedSort;
        this.ignoreUnmapped = original.ignoreUnmapped;
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public GeoDistanceSortBuilder(StreamInput in) throws IOException {
        fieldName = in.readString();
        points.addAll((List<GeoPoint>) in.readGenericValue());
        geoDistance = GeoDistance.readFromStream(in);
        unit = DistanceUnit.readFromStream(in);
        order = SortOrder.readFromStream(in);
        sortMode = in.readOptionalWriteable(SortMode::readFromStream);
        if (in.getVersion().before(Version.V_8_0_0)) {
            if (in.readOptionalNamedWriteable(QueryBuilder.class) != null || in.readOptionalString() != null) {
                throw new IOException(
                    "the [sort] options [nested_path] and [nested_filter] are removed in 8.x, " + "please use [nested] instead"
                );
            }

        }
        nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
        validation = GeoValidationMethod.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(points);
        geoDistance.writeTo(out);
        unit.writeTo(out);
        order.writeTo(out);
        out.writeOptionalWriteable(sortMode);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeOptionalNamedWriteable(null);
            out.writeOptionalString(null);
        }
        out.writeOptionalWriteable(nestedSort);
        validation.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * Returns the geo point like field the distance based sort operates on.
     * */
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * The point to create the range distance facets from.
     *
     * @param lat latitude.
     * @param lon longitude.
     */
    public GeoDistanceSortBuilder point(double lat, double lon) {
        points.add(new GeoPoint(lat, lon));
        return this;
    }

    /**
     * The point to create the range distance facets from.
     *
     * @param points reference points.
     */
    public GeoDistanceSortBuilder points(GeoPoint... points) {
        this.points.addAll(Arrays.asList(points));
        return this;
    }

    /**
     * Returns the points to create the range distance facets from.
     */
    public GeoPoint[] points() {
        return this.points.toArray(new GeoPoint[this.points.size()]);
    }

    /**
     * The geo distance type used to compute the distance.
     */
    public GeoDistanceSortBuilder geoDistance(GeoDistance geoDistance) {
        this.geoDistance = geoDistance;
        return this;
    }

    /**
     * Returns the geo distance type used to compute the distance.
     */
    public GeoDistance geoDistance() {
        return this.geoDistance;
    }

    /**
     * The distance unit to use. Defaults to {@link org.elasticsearch.common.unit.DistanceUnit#METERS}
     */
    public GeoDistanceSortBuilder unit(DistanceUnit unit) {
        this.unit = unit;
        return this;
    }

    /**
     * Returns the distance unit to use. Defaults to {@link org.elasticsearch.common.unit.DistanceUnit#METERS}
     */
    public DistanceUnit unit() {
        return this.unit;
    }

    /**
     * Sets validation method for this sort builder.
     */
    public GeoDistanceSortBuilder validation(GeoValidationMethod method) {
        this.validation = method;
        return this;
    }

    /**
     * Returns the validation method to use for this sort builder.
     */
    public GeoValidationMethod validation() {
        return validation;
    }

    /**
     * Defines which distance to use for sorting in the case a document contains multiple geo points.
     * Possible values: min and max
     */
    public GeoDistanceSortBuilder sortMode(SortMode sortMode) {
        Objects.requireNonNull(sortMode, "sort mode cannot be null");
        if (sortMode == SortMode.SUM) {
            throw new IllegalArgumentException("sort_mode [sum] isn't supported for sorting by geo distance");
        }
        this.sortMode = sortMode;
        return this;
    }

    /** Returns which distance to use for sorting in the case a document contains multiple geo points. */
    public SortMode sortMode() {
        return this.sortMode;
    }

    /**
     * Returns the {@link NestedSortBuilder}
     */
    public NestedSortBuilder getNestedSort() {
        return this.nestedSort;
    }

    /**
     * Sets the {@link NestedSortBuilder} to be used for fields that are inside a nested
     * object. The {@link NestedSortBuilder} takes a `path` argument and an optional
     * nested filter that the nested objects should match with in
     * order to be taken into account for sorting.
     */
    public GeoDistanceSortBuilder setNestedSort(final NestedSortBuilder nestedSort) {
        this.nestedSort = nestedSort;
        return this;
    }

    /**
     * Returns true if unmapped geo fields should be treated as located at an infinite distance
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    public GeoDistanceSortBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);

        builder.startArray(fieldName);
        for (GeoPoint point : points) {
            builder.value(point);
        }
        builder.endArray();

        builder.field(UNIT_FIELD.getPreferredName(), unit);
        builder.field(DISTANCE_TYPE_FIELD.getPreferredName(), geoDistance.name().toLowerCase(Locale.ROOT));
        builder.field(ORDER_FIELD.getPreferredName(), order);

        if (sortMode != null) {
            builder.field(SORTMODE_FIELD.getPreferredName(), sortMode);
        }

        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        builder.field(VALIDATION_METHOD_FIELD.getPreferredName(), validation);
        builder.field(IGNORE_UNMAPPED.getPreferredName(), ignoreUnmapped);

        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        GeoDistanceSortBuilder other = (GeoDistanceSortBuilder) object;
        return Objects.equals(fieldName, other.fieldName)
            && Objects.deepEquals(points, other.points)
            && Objects.equals(geoDistance, other.geoDistance)
            && Objects.equals(unit, other.unit)
            && Objects.equals(sortMode, other.sortMode)
            && Objects.equals(order, other.order)
            && Objects.equals(validation, other.validation)
            && Objects.equals(nestedSort, other.nestedSort)
            && ignoreUnmapped == other.ignoreUnmapped;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            this.fieldName,
            this.points,
            this.geoDistance,
            this.unit,
            this.sortMode,
            this.order,
            this.validation,
            this.nestedSort,
            this.ignoreUnmapped
        );
    }

    /**
     * Creates a new {@link GeoDistanceSortBuilder} from the query held by the {@link XContentParser} in
     * {@link org.elasticsearch.xcontent.XContent} format.
     *
     * @param parser the input parser. The state on the parser contained in this context will be changed as a
     *                side effect of this method call
     * @param elementName in some sort syntax variations the field name precedes the xContent object that specifies
     *                    further parameters, e.g. in '{ "foo": { "order" : "asc"} }'. When parsing the inner object,
     *                    the field name can be passed in via this argument
     */
    public static GeoDistanceSortBuilder fromXContent(XContentParser parser, String elementName) throws IOException {
        String fieldName = null;
        List<GeoPoint> geoPoints = new ArrayList<>();
        DistanceUnit unit = DistanceUnit.DEFAULT;
        GeoDistance geoDistance = GeoDistance.ARC;
        SortOrder order = SortOrder.ASC;
        SortMode sortMode = null;
        NestedSortBuilder nestedSort = null;
        GeoValidationMethod validation = null;
        boolean ignoreUnmapped = false;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseGeoPoints(parser, geoPoints);

                fieldName = currentName;
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parser.getRestApiVersion() == RestApiVersion.V_7
                    && NESTED_FILTER_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    deprecationLogger.compatibleCritical(
                        "nested_filter",
                        "[nested_filter] has been removed in favour of the [nested] parameter"
                    );
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[nested_filter] has been removed in favour of the [nested] parameter",
                        currentName
                    );
                } else if (NESTED_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    nestedSort = NestedSortBuilder.fromXContent(parser);
                } else {
                    // the json in the format of -> field : { lat : 30, lon : 12 }
                    if (fieldName != null && fieldName.equals(currentName) == false) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Trying to reset fieldName to [{}], already set to [{}].",
                            currentName,
                            fieldName
                        );
                    }
                    fieldName = currentName;
                    geoPoints.add(GeoUtils.parseGeoPoint(parser));
                }
            } else if (token.isValue()) {
                if (parser.getRestApiVersion() == RestApiVersion.V_7
                    && NESTED_PATH_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    deprecationLogger.compatibleCritical(
                        "nested_path",
                        "[nested_path] has been removed in favour of the [nested] parameter"
                    );
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[nested_path] has been removed in favour of the [nested] parameter",
                        currentName
                    );
                } else if (ORDER_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    order = SortOrder.fromString(parser.text());
                } else if (UNIT_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (DISTANCE_TYPE_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (VALIDATION_METHOD_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    validation = GeoValidationMethod.fromString(parser.text());
                } else if (SORTMODE_FIELD.match(currentName, parser.getDeprecationHandler())) {
                    sortMode = SortMode.fromString(parser.text());
                } else if (IGNORE_UNMAPPED.match(currentName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (token == Token.VALUE_STRING) {
                    if (fieldName != null && fieldName.equals(currentName) == false) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Trying to reset fieldName to [{}], already set to [{}].",
                            currentName,
                            fieldName
                        );
                    }

                    GeoPoint point = new GeoPoint();
                    point.resetFromString(parser.text());
                    geoPoints.add(point);
                    fieldName = currentName;
                } else if (fieldName.equals(currentName)) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Only geohashes of type string supported for field [{}]",
                        currentName
                    );
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[{}] does not support [{}]", NAME, currentName);
                }
            }
        }

        GeoDistanceSortBuilder result = new GeoDistanceSortBuilder(fieldName, geoPoints.toArray(new GeoPoint[geoPoints.size()]));
        result.geoDistance(geoDistance);
        result.unit(unit);
        result.order(order);
        if (sortMode != null) {
            result.sortMode(sortMode);
        }
        if (nestedSort != null) {
            result.setNestedSort(nestedSort);
        }
        if (validation != null) {
            result.validation(validation);
        }
        result.ignoreUnmapped(ignoreUnmapped);
        return result;
    }

    @Override
    public SortFieldAndFormat build(SearchExecutionContext context) throws IOException {
        GeoPoint[] localPoints = localPoints();
        boolean reverse = order == SortOrder.DESC;
        MultiValueMode localSortMode = localSortMode();
        IndexGeoPointFieldData geoIndexFieldData = fieldData(context);
        Nested nested = nested(context);

        if (geoIndexFieldData.getClass() == LatLonPointIndexFieldData.class // only works with 5.x geo_point
            && nested == null
            && localSortMode == MultiValueMode.MIN // LatLonDocValuesField internally picks the closest point
            && unit == DistanceUnit.METERS
            && reverse == false
            && localPoints.length == 1) {
            return new SortFieldAndFormat(
                LatLonDocValuesField.newDistanceSort(fieldName, localPoints[0].lat(), localPoints[0].lon()),
                DocValueFormat.RAW
            );
        }

        return new SortFieldAndFormat(
            new SortField(fieldName, comparatorSource(localPoints, localSortMode, geoIndexFieldData, nested), reverse),
            DocValueFormat.RAW
        );
    }

    @Override
    public BucketedSort buildBucketedSort(SearchExecutionContext context, BigArrays bigArrays, int bucketSize, BucketedSort.ExtraData extra)
        throws IOException {
        GeoPoint[] localPoints = localPoints();
        MultiValueMode localSortMode = localSortMode();
        IndexGeoPointFieldData geoIndexFieldData = fieldData(context);
        Nested nested = nested(context);

        // TODO implement the single point optimization above

        return comparatorSource(localPoints, localSortMode, geoIndexFieldData, nested).newBucketedSort(
            bigArrays,
            order,
            DocValueFormat.RAW,
            bucketSize,
            extra
        );
    }

    private GeoPoint[] localPoints() {
        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed
        // on 2.x created indexes
        GeoPoint[] localPoints = points.toArray(new GeoPoint[points.size()]);
        if (GeoValidationMethod.isIgnoreMalformed(validation) == false) {
            for (GeoPoint point : localPoints) {
                if (GeoUtils.isValidLatitude(point.lat()) == false) {
                    throw new ElasticsearchParseException(
                        "illegal latitude value [{}] for [GeoDistanceSort] for field [{}].",
                        point.lat(),
                        fieldName
                    );
                }
                if (GeoUtils.isValidLongitude(point.lon()) == false) {
                    throw new ElasticsearchParseException(
                        "illegal longitude value [{}] for [GeoDistanceSort] for field [{}].",
                        point.lon(),
                        fieldName
                    );
                }
            }
        }

        if (GeoValidationMethod.isCoerce(validation)) {
            for (GeoPoint point : localPoints) {
                GeoUtils.normalizePoint(point, true, true);
            }
        }
        return localPoints;
    }

    private MultiValueMode localSortMode() {
        // TODO this lines up with FieldSortBuilder. Share?
        if (sortMode != null) {
            return MultiValueMode.fromString(sortMode.toString());
        }

        return order == SortOrder.DESC ? MultiValueMode.MAX : MultiValueMode.MIN;
    }

    private IndexGeoPointFieldData fieldData(SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new LatLonPointIndexFieldData(
                    fieldName,
                    CoreValuesSourceType.GEOPOINT,
                    // we do not support scripting for unmapped sorts
                    (dv, n) -> { throw new UnsupportedOperationException(); }
                );
            } else {
                throw new IllegalArgumentException("failed to find mapper for [" + fieldName + "] for geo distance based sort");
            }
        }
        return context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
    }

    private Nested nested(SearchExecutionContext context) throws IOException {
        // TODO this is pretty similar to FieldSortBuilder. Share?
        if (nestedSort == null) {
            validateMissingNestedPath(context, fieldName);
            return null;
        }
        validateMaxChildrenExistOnlyInTopLevelNestedSort(context, nestedSort);
        return resolveNested(context, nestedSort);
    }

    private IndexFieldData.XFieldComparatorSource comparatorSource(
        GeoPoint[] localPoints,
        MultiValueMode localSortMode,
        IndexGeoPointFieldData geoIndexFieldData,
        Nested nested
    ) {
        return new IndexFieldData.XFieldComparatorSource(null, localSortMode, nested) {
            @Override
            public SortField.Type reducedType() {
                return SortField.Type.DOUBLE;
            }

            private NumericDoubleValues getNumericDoubleValues(LeafReaderContext context) throws IOException {
                final MultiGeoPointValues geoPointValues = geoIndexFieldData.load(context).getGeoPointValues();
                final SortedNumericDoubleValues distanceValues = GeoUtils.distanceValues(geoDistance, unit, geoPointValues, localPoints);
                if (nested == null) {
                    return FieldData.replaceMissing(sortMode.select(distanceValues), Double.POSITIVE_INFINITY);
                } else {
                    final BitSet rootDocs = nested.rootDocs(context);
                    final DocIdSetIterator innerDocs = nested.innerDocs(context);
                    final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
                    return localSortMode.select(
                        distanceValues,
                        Double.POSITIVE_INFINITY,
                        rootDocs,
                        innerDocs,
                        context.reader().maxDoc(),
                        maxChildren
                    );
                }
            }

            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
                return new DoubleComparator(numHits, null, null, reversed, false) {
                    @Override
                    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                        return new DoubleLeafComparator(context) {
                            @Override
                            protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                                return getNumericDoubleValues(context).getRawDoubleValues();
                            }
                        };
                    }
                };
            }

            @Override
            public BucketedSort newBucketedSort(
                BigArrays bigArrays,
                SortOrder sortOrder,
                DocValueFormat format,
                int bucketSize,
                BucketedSort.ExtraData extra
            ) {
                return new BucketedSort.ForDoubles(bigArrays, sortOrder, format, bucketSize, extra) {
                    @Override
                    public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                        return new Leaf(ctx) {
                            private final NumericDoubleValues values = getNumericDoubleValues(ctx);
                            private double value;

                            @Override
                            protected boolean advanceExact(int doc) throws IOException {
                                if (values.advanceExact(doc)) {
                                    value = values.doubleValue();
                                    return true;
                                }
                                return false;
                            }

                            @Override
                            protected double docValue() {
                                return value;
                            }
                        };
                    }
                };
            }
        };
    }

    static void parseGeoPoints(XContentParser parser, List<GeoPoint> geoPoints) throws IOException {
        while (parser.nextToken().equals(XContentParser.Token.END_ARRAY) == false) {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                // we might get here if the geo point is " number, number] " and the parser already moved over the
                // opening bracket in this case we cannot use GeoUtils.parseGeoPoint(..) because this expects an opening
                // bracket
                double lon = parser.doubleValue();
                parser.nextToken();
                if (parser.currentToken().equals(XContentParser.Token.VALUE_NUMBER) == false) {
                    throw new ElasticsearchParseException(
                        "geo point parsing: expected second number but got [{}] instead",
                        parser.currentToken()
                    );
                }
                double lat = parser.doubleValue();
                geoPoints.add(new GeoPoint(lat, lon));
            } else {
                geoPoints.add(GeoUtils.parseGeoPoint(parser));
            }

        }
    }

    @Override
    public GeoDistanceSortBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (nestedSort == null) {
            return this;
        }
        NestedSortBuilder rewrite = nestedSort.rewrite(ctx);
        if (nestedSort == rewrite) {
            return this;
        }
        return new GeoDistanceSortBuilder(this).setNestedSort(rewrite);
    }
}
