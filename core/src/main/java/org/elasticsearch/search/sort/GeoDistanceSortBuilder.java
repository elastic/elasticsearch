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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * A geo distance based sorting on a geo point like field.
 */
public class GeoDistanceSortBuilder extends SortBuilder implements SortBuilderTemp<GeoDistanceSortBuilder> {
    public static final String NAME = "_geo_distance";
    public static final boolean DEFAULT_COERCE = false;
    public static final boolean DEFAULT_IGNORE_MALFORMED = false;

    static final GeoDistanceSortBuilder PROTOTYPE = new GeoDistanceSortBuilder("", -1, -1);

    private final String fieldName;
    private final List<GeoPoint> points = new ArrayList<>();

    private GeoDistance geoDistance = GeoDistance.DEFAULT;
    private DistanceUnit unit = DistanceUnit.DEFAULT;

    // TODO there is an enum that covers that parameter which we should be using here
    private String sortMode = null;
    @SuppressWarnings("rawtypes")
    private QueryBuilder nestedFilter;
    private String nestedPath;

    // TODO switch to GeoValidationMethod enum
    private boolean coerce = DEFAULT_COERCE;
    private boolean ignoreMalformed = DEFAULT_IGNORE_MALFORMED;

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
        this.coerce = original.coerce;
        this.ignoreMalformed = original.ignoreMalformed;
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
     * Defines which distance to use for sorting in the case a document contains multiple geo points.
     * Possible values: min and max
     */
    public GeoDistanceSortBuilder sortMode(String sortMode) {
        MultiValueMode temp = MultiValueMode.fromString(sortMode);
        if (temp == MultiValueMode.SUM) {
            throw new IllegalArgumentException("sort_mode [sum] isn't supported for sorting by geo distance");
        }
        this.sortMode = sortMode;
        return this;
    }

    /** Returns which distance to use for sorting in the case a document contains multiple geo points. */
    public String sortMode() {
        return this.sortMode;
    }

    /**
     * Sets the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     */
    public GeoDistanceSortBuilder setNestedFilter(QueryBuilder<?> nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Returns the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     **/
    public QueryBuilder<?> getNestedFilter() {
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

    public GeoDistanceSortBuilder coerce(boolean coerce) {
        this.coerce = coerce;
        return this;
    }

    public boolean coerce() {
        return this.coerce;
    }

    public GeoDistanceSortBuilder ignoreMalformed(boolean ignoreMalformed) {
        if (coerce == false) {
            this.ignoreMalformed = ignoreMalformed;
        }
        return this;
    }

    public boolean ignoreMalformed() {
        return this.ignoreMalformed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startArray(fieldName);
        for (GeoPoint point : points) {
            builder.value(point);
        }
        builder.endArray();

        builder.field("unit", unit);
        builder.field("distance_type", geoDistance.name().toLowerCase(Locale.ROOT));
        builder.field(ORDER_FIELD.getPreferredName(), order);

        if (sortMode != null) {
            builder.field("mode", sortMode);
        }

        if (nestedPath != null) {
            builder.field("nested_path", nestedPath);
        }
        if (nestedFilter != null) {
            builder.field("nested_filter", nestedFilter, params);
        }
        builder.field("coerce", coerce);
        builder.field("ignore_malformed", ignoreMalformed);

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
                Objects.equals(coerce, other.coerce) &&
                Objects.equals(ignoreMalformed, other.ignoreMalformed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fieldName, this.points, this.geoDistance,
                this.unit, this.sortMode, this.order, this.nestedFilter, this.nestedPath, this.coerce, this.ignoreMalformed);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(points);

        geoDistance.writeTo(out);
        unit.writeTo(out);
        order.writeTo(out);
        out.writeOptionalString(sortMode);
        if (nestedFilter != null) {
            out.writeBoolean(true);
            out.writeQuery(nestedFilter);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(nestedPath);
        out.writeBoolean(coerce);
        out.writeBoolean(ignoreMalformed);
    }

    @Override
    public GeoDistanceSortBuilder readFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();

        ArrayList<GeoPoint> points = (ArrayList<GeoPoint>) in.readGenericValue();
        GeoDistanceSortBuilder result = new GeoDistanceSortBuilder(fieldName, points.toArray(new GeoPoint[points.size()]));

        result.geoDistance(GeoDistance.readGeoDistanceFrom(in));
        result.unit(DistanceUnit.readDistanceUnit(in));
        result.order(SortOrder.readOrderFrom(in));
        String sortMode = in.readOptionalString();
        if (sortMode != null) {
            result.sortMode(sortMode);
        }
        if (in.readBoolean()) {
            result.setNestedFilter(in.readQuery());
        }
        result.setNestedPath(in.readOptionalString());
        result.coerce(in.readBoolean());
        result.ignoreMalformed(in.readBoolean());
        return result;
    }

    @Override
    public GeoDistanceSortBuilder fromXContent(QueryParseContext context, String elementName) throws IOException {
        XContentParser parser = context.parser();
        String fieldName = null;
        List<GeoPoint> geoPoints = new ArrayList<>();
        DistanceUnit unit = DistanceUnit.DEFAULT;
        GeoDistance geoDistance = GeoDistance.DEFAULT;
        SortOrder order = SortOrder.ASC;
        MultiValueMode sortMode = null;
        QueryBuilder<?> nestedFilter = null;
        String nestedPath = null;

        boolean coerce = GeoDistanceSortBuilder.DEFAULT_COERCE;
        boolean ignoreMalformed = GeoDistanceSortBuilder.DEFAULT_IGNORE_MALFORMED;

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                parseGeoPoints(parser, geoPoints);

                fieldName = currentName;
            } else if (token == XContentParser.Token.START_OBJECT) {
                // the json in the format of -> field : { lat : 30, lon : 12 }
                if ("nested_filter".equals(currentName) || "nestedFilter".equals(currentName)) {
                    // TODO Note to remember: while this is kept as a QueryBuilder internally,
                    // we need to make sure to call toFilter() on it once on the shard
                    // (e.g. in the new build() method)
                    nestedFilter = context.parseInnerQueryBuilder();
                } else {
                    fieldName = currentName;
                    GeoPoint point = new GeoPoint();
                    GeoUtils.parseGeoPoint(parser, point);
                    geoPoints.add(point);
                }
            } else if (token.isValue()) {
                if ("reverse".equals(currentName)) {
                    order = parser.booleanValue() ? SortOrder.DESC : SortOrder.ASC;
                } else if ("order".equals(currentName)) {
                    order = SortOrder.fromString(parser.text());
                } else if ("unit".equals(currentName)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if ("distance_type".equals(currentName) || "distanceType".equals(currentName)) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if ("coerce".equals(currentName) || "normalize".equals(currentName)) {
                    coerce = parser.booleanValue();
                    if (coerce == true) {
                        ignoreMalformed = true;
                    }
                } else if ("ignore_malformed".equals(currentName)) {
                    boolean ignore_malformed_value = parser.booleanValue();
                    if (coerce == false) {
                        ignoreMalformed = ignore_malformed_value;
                    }
                } else if ("sort_mode".equals(currentName) || "sortMode".equals(currentName) || "mode".equals(currentName)) {
                    sortMode = MultiValueMode.fromString(parser.text());
                } else if ("nested_path".equals(currentName) || "nestedPath".equals(currentName)) {
                    nestedPath = parser.text();
                } else {
                    GeoPoint point = new GeoPoint();
                    point.resetFromString(parser.text());
                    geoPoints.add(point);
                    fieldName = currentName;
                }
            }
        }

        GeoDistanceSortBuilder result = new GeoDistanceSortBuilder(fieldName, geoPoints.toArray(new GeoPoint[geoPoints.size()]));
        result.geoDistance(geoDistance);
        result.unit(unit);
        result.order(order);
        if (sortMode != null) {
            result.sortMode(sortMode.name());
        }
        result.setNestedFilter(nestedFilter);
        result.setNestedPath(nestedPath);
        result.coerce(coerce);
        result.ignoreMalformed(ignoreMalformed);
        return result;

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
