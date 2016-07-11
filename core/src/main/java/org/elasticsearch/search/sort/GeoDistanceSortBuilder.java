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

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoDistance.FixedSourceDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * A geo distance based sorting on a geo point like field.
 */
public class GeoDistanceSortBuilder extends SortBuilder<GeoDistanceSortBuilder> {
    public static final String NAME = "_geo_distance";
    public static final String ALTERNATIVE_NAME = "_geoDistance";
    public static final GeoValidationMethod DEFAULT_VALIDATION = GeoValidationMethod.DEFAULT;

    private static final ParseField UNIT_FIELD = new ParseField("unit");
    private static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");
    private static final ParseField VALIDATION_METHOD_FIELD = new ParseField("validation_method");
    private static final ParseField IGNORE_MALFORMED_FIELD = new ParseField("ignore_malformed")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField COERCE_FIELD = new ParseField("coerce", "normalize")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField SORTMODE_FIELD = new ParseField("mode", "sort_mode");
    private static final ParseField NESTED_PATH_FIELD = new ParseField("nested_path");
    private static final ParseField NESTED_FILTER_FIELD = new ParseField("nested_filter");

    private final String fieldName;
    private final List<GeoPoint> points = new ArrayList<>();

    private GeoDistance geoDistance = GeoDistance.DEFAULT;
    private DistanceUnit unit = DistanceUnit.DEFAULT;

    private SortMode sortMode = null;
    @SuppressWarnings("rawtypes")
    private QueryBuilder nestedFilter;
    private String nestedPath;

    private GeoValidationMethod validation = DEFAULT_VALIDATION;

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
    public GeoDistanceSortBuilder(String fieldName, String ... geohashes) {
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
        this.nestedFilter = original.nestedFilter;
        this.nestedPath = original.nestedPath;
        this.validation = original.validation;
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
        nestedFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nestedPath = in.readOptionalString();
        validation = GeoValidationMethod.readFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(points);
        geoDistance.writeTo(out);
        unit.writeTo(out);
        order.writeTo(out);
        out.writeOptionalWriteable(sortMode);
        out.writeOptionalNamedWriteable(nestedFilter);
        out.writeOptionalString(nestedPath);
        validation.writeTo(out);
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
     * The geohash of the geo point to create the range distance facets from.
     *
     * Deprecated - please use points(GeoPoint... points) instead.
     */
    @Deprecated
    public GeoDistanceSortBuilder geohashes(String... geohashes) {
        for (String geohash : geohashes) {
            this.points.add(GeoPoint.fromGeohash(geohash));
        }
        return this;
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
     * The distance unit to use. Defaults to {@link org.elasticsearch.common.unit.DistanceUnit#KILOMETERS}
     */
    public GeoDistanceSortBuilder unit(DistanceUnit unit) {
        this.unit = unit;
        return this;
    }

    /**
     * Returns the distance unit to use. Defaults to {@link org.elasticsearch.common.unit.DistanceUnit#KILOMETERS}
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
     * Sets the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     */
    public GeoDistanceSortBuilder setNestedFilter(QueryBuilder nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Returns the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     **/
    public QueryBuilder getNestedFilter() {
        return this.nestedFilter;
    }

    /**
     * Sets the nested path if sorting occurs on a field that is inside a nested object. By default when sorting on a
     * field inside a nested object, the nearest upper nested object is selected as nested path.
     */
    public GeoDistanceSortBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
    }

    /**
     * Returns the nested path if sorting occurs on a field that is inside a nested object. By default when sorting on a
     * field inside a nested object, the nearest upper nested object is selected as nested path.
     */
    public String getNestedPath() {
        return this.nestedPath;
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

        if (nestedPath != null) {
            builder.field(NESTED_PATH_FIELD.getPreferredName(), nestedPath);
        }
        if (nestedFilter != null) {
            builder.field(NESTED_FILTER_FIELD.getPreferredName(), nestedFilter, params);
        }
        builder.field(VALIDATION_METHOD_FIELD.getPreferredName(), validation);

        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.deepEquals(points, other.points) &&
                Objects.equals(geoDistance, other.geoDistance) &&
                Objects.equals(unit, other.unit) &&
                Objects.equals(sortMode, other.sortMode) &&
                Objects.equals(order, other.order) &&
                Objects.equals(nestedFilter, other.nestedFilter) &&
                Objects.equals(nestedPath, other.nestedPath) &&
                Objects.equals(validation, other.validation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fieldName, this.points, this.geoDistance,
                this.unit, this.sortMode, this.order, this.nestedFilter,
                this.nestedPath, this.validation);
    }

    /**
     * Creates a new {@link GeoDistanceSortBuilder} from the query held by the {@link QueryParseContext} in
     * {@link org.elasticsearch.common.xcontent.XContent} format.
     *
     * @param context the input parse context. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param elementName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static GeoDistanceSortBuilder fromXContent(QueryParseContext context, String elementName) throws IOException {
        XContentParser parser = context.parser();
        ParseFieldMatcher parseFieldMatcher = context.getParseFieldMatcher();
        String fieldName = null;
        List<GeoPoint> geoPoints = new ArrayList<>();
        DistanceUnit unit = DistanceUnit.DEFAULT;
        GeoDistance geoDistance = GeoDistance.DEFAULT;
        SortOrder order = SortOrder.ASC;
        SortMode sortMode = null;
        Optional<QueryBuilder> nestedFilter = Optional.empty();
        String nestedPath = null;

        boolean coerce = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        boolean ignoreMalformed = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        GeoValidationMethod validation = null;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseGeoPoints(parser, geoPoints);

                fieldName = currentName;
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseFieldMatcher.match(currentName, NESTED_FILTER_FIELD)) {
                    nestedFilter = context.parseInnerQueryBuilder();
                } else {
                    // the json in the format of -> field : { lat : 30, lon : 12 }
                    if (fieldName != null && fieldName.equals(currentName) == false) {
                        throw new ParsingException(
                                parser.getTokenLocation(),
                                "Trying to reset fieldName to [{}], already set to [{}].",
                                currentName,
                                fieldName);
                    }
                    fieldName = currentName;
                    GeoPoint point = new GeoPoint();
                    GeoUtils.parseGeoPoint(parser, point);
                    geoPoints.add(point);
                }
            } else if (token.isValue()) {
                if (parseFieldMatcher.match(currentName, ORDER_FIELD)) {
                    order = SortOrder.fromString(parser.text());
                } else if (parseFieldMatcher.match(currentName, UNIT_FIELD)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (parseFieldMatcher.match(currentName, DISTANCE_TYPE_FIELD)) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if (parseFieldMatcher.match(currentName, COERCE_FIELD)) {
                    coerce = parser.booleanValue();
                    if (coerce == true) {
                        ignoreMalformed = true;
                    }
                } else if (parseFieldMatcher.match(currentName, IGNORE_MALFORMED_FIELD)) {
                    boolean ignore_malformed_value = parser.booleanValue();
                    if (coerce == false) {
                        ignoreMalformed = ignore_malformed_value;
                    }
                } else if (parseFieldMatcher.match(currentName, VALIDATION_METHOD_FIELD)) {
                    validation = GeoValidationMethod.fromString(parser.text());
                } else if (parseFieldMatcher.match(currentName, SORTMODE_FIELD)) {
                    sortMode = SortMode.fromString(parser.text());
                } else if (parseFieldMatcher.match(currentName, NESTED_PATH_FIELD)) {
                    nestedPath = parser.text();
                } else if (token == Token.VALUE_STRING){
                    if (fieldName != null && fieldName.equals(currentName) == false) {
                        throw new ParsingException(
                                parser.getTokenLocation(),
                                "Trying to reset fieldName to [{}], already set to [{}].",
                                currentName,
                                fieldName);
                    }

                    GeoPoint point = new GeoPoint();
                    point.resetFromString(parser.text());
                    geoPoints.add(point);
                    fieldName = currentName;
                } else {
                    throw new ParsingException(
                            parser.getTokenLocation(),
                            "Only geohashes of type string supported for field [{}]",
                            currentName);
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
        nestedFilter.ifPresent(result::setNestedFilter);
        result.setNestedPath(nestedPath);
        if (validation == null) {
            // looks like either validation was left unset or we are parsing old validation json
            result.validation(GeoValidationMethod.infer(coerce, ignoreMalformed));
        } else {
            // ignore deprecated coerce/ignore_malformed
            result.validation(validation);
        }
        return result;
    }

    @Override
    public SortFieldAndFormat build(QueryShardContext context) throws IOException {
        final boolean indexCreatedBeforeV2_0 = context.indexVersionCreated().before(Version.V_2_0_0);
        // validation was not available prior to 2.x, so to support bwc percolation queries we only ignore_malformed on 2.x created indexes
        List<GeoPoint> localPoints = new ArrayList<GeoPoint>();
        for (GeoPoint geoPoint : this.points) {
            localPoints.add(new GeoPoint(geoPoint));
        }

        if (!indexCreatedBeforeV2_0 && !GeoValidationMethod.isIgnoreMalformed(validation)) {
            for (GeoPoint point : localPoints) {
                if (GeoUtils.isValidLatitude(point.lat()) == false) {
                    throw new ElasticsearchParseException(
                            "illegal latitude value [{}] for [GeoDistanceSort] for field [{}].",
                            point.lat(),
                            fieldName);
                }
                if (GeoUtils.isValidLongitude(point.lon()) == false) {
                    throw new ElasticsearchParseException(
                            "illegal longitude value [{}] for [GeoDistanceSort] for field [{}].",
                            point.lon(),
                            fieldName);
                }
            }
        }

        if (GeoValidationMethod.isCoerce(validation)) {
            for (GeoPoint point : localPoints) {
                GeoUtils.normalizePoint(point, true, true);
            }
        }

        boolean reverse = (order == SortOrder.DESC);
        final MultiValueMode finalSortMode;
        if (sortMode == null) {
            finalSortMode = reverse ? MultiValueMode.MAX : MultiValueMode.MIN;
        } else {
            finalSortMode = MultiValueMode.fromString(sortMode.toString());
        }

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new IllegalArgumentException("failed to find mapper for [" + fieldName + "] for geo distance based sort");
        }
        final IndexGeoPointFieldData geoIndexFieldData = context.getForField(fieldType);
        final FixedSourceDistance[] distances = new FixedSourceDistance[localPoints.size()];
        for (int i = 0; i< localPoints.size(); i++) {
            distances[i] = geoDistance.fixedSourceDistance(localPoints.get(i).lat(), localPoints.get(i).lon(), unit);
        }

        final Nested nested = resolveNested(context, nestedPath, nestedFilter);

        IndexFieldData.XFieldComparatorSource geoDistanceComparatorSource = new IndexFieldData.XFieldComparatorSource() {

            @Override
            public SortField.Type reducedType() {
                return SortField.Type.DOUBLE;
            }

            @Override
            public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
                return new FieldComparator.DoubleComparator(numHits, null, null) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        final MultiGeoPointValues geoPointValues = geoIndexFieldData.load(context).getGeoPointValues();
                        final SortedNumericDoubleValues distanceValues = GeoDistance.distanceValues(geoPointValues, distances);
                        final NumericDoubleValues selectedValues;
                        if (nested == null) {
                            selectedValues = finalSortMode.select(distanceValues, Double.MAX_VALUE);
                        } else {
                            final BitSet rootDocs = nested.rootDocs(context);
                            final DocIdSetIterator innerDocs = nested.innerDocs(context);
                            selectedValues = finalSortMode.select(distanceValues, Double.MAX_VALUE, rootDocs, innerDocs,
                                    context.reader().maxDoc());
                        }
                        return selectedValues.getRawDoubleValues();
                    }
                };
            }

        };

        return new SortFieldAndFormat(new SortField(fieldName, geoDistanceComparatorSource, reverse), DocValueFormat.RAW);
    }

    static void parseGeoPoints(XContentParser parser, List<GeoPoint> geoPoints) throws IOException {
        while (!parser.nextToken().equals(XContentParser.Token.END_ARRAY)) {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                // we might get here if the geo point is " number, number] " and the parser already moved over the opening bracket
                // in this case we cannot use GeoUtils.parseGeoPoint(..) because this expects an opening bracket
                double lon = parser.doubleValue();
                parser.nextToken();
                if (!parser.currentToken().equals(XContentParser.Token.VALUE_NUMBER)) {
                    throw new ElasticsearchParseException(
                            "geo point parsing: expected second number but got [{}] instead",
                            parser.currentToken());
                }
                double lat = parser.doubleValue();
                GeoPoint point = new GeoPoint();
                point.reset(lat, lon);
                geoPoints.add(point);
            } else {
                GeoPoint point = new GeoPoint();
                GeoUtils.parseGeoPoint(parser, point);
                geoPoints.add(point);
            }

        }
    }
}
