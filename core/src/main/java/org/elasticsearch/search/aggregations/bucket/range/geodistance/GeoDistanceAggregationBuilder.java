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

package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser.Range;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GeoDistanceAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoDistanceAggregationBuilder> {
    public static final String NAME = InternalGeoDistance.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    private final GeoPoint origin;
    private List<Range> ranges = new ArrayList<>();
    private DistanceUnit unit = DistanceUnit.DEFAULT;
    private GeoDistance distanceType = GeoDistance.DEFAULT;
    private boolean keyed = false;

    public GeoDistanceAggregationBuilder(String name, GeoPoint origin) {
        this(name, origin, InternalGeoDistance.FACTORY);
    }

    private GeoDistanceAggregationBuilder(String name, GeoPoint origin,
                                          InternalRange.Factory<InternalGeoDistance.Bucket, InternalGeoDistance> rangeFactory) {
        super(name, rangeFactory.type(), rangeFactory.getValueSourceType(), rangeFactory.getValueType());
        if (origin == null) {
            throw new IllegalArgumentException("[origin] must not be null: [" + name + "]");
        }
        this.origin = origin;
    }

    /**
     * Read from a stream.
     */
    public GeoDistanceAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalGeoDistance.FACTORY.type(), InternalGeoDistance.FACTORY.getValueSourceType(),
                InternalGeoDistance.FACTORY.getValueType());
        origin = new GeoPoint(in.readDouble(), in.readDouble());
        int size = in.readVInt();
        ranges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ranges.add(new Range(in));
        }
        keyed = in.readBoolean();
        distanceType = GeoDistance.readFromStream(in);
        unit = DistanceUnit.readFromStream(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(origin.lat());
        out.writeDouble(origin.lon());
        out.writeVInt(ranges.size());
        for (Range range : ranges) {
            range.writeTo(out);
        }
        out.writeBoolean(keyed);
        distanceType.writeTo(out);
        unit.writeTo(out);
    }

    public GeoDistanceAggregationBuilder addRange(Range range) {
        if (range == null) {
            throw new IllegalArgumentException("[range] must not be null: [" + name + "]");
        }
        ranges.add(range);
        return this;
    }

    /**
     * Add a new range to this aggregation.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     * @param to
     *            the upper bound on the distances, exclusive
     */
    public GeoDistanceAggregationBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addRange(String, double, double)} but the key will be
     * automatically generated based on <code>from</code> and
     * <code>to</code>.
     */
    public GeoDistanceAggregationBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    /**
     * Add a new range with no lower bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param to
     *            the upper bound on the distances, exclusive
     */
    public GeoDistanceAggregationBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, double)} but the key will be
     * computed automatically.
     */
    public GeoDistanceAggregationBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Add a new range with no upper bound.
     *
     * @param key
     *            the key to use for this range in the response
     * @param from
     *            the lower bound on the distances, inclusive
     */
    public GeoDistanceAggregationBuilder addUnboundedFrom(String key, double from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, double)} but the key will be
     * computed automatically.
     */
    public GeoDistanceAggregationBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

    public List<Range> range() {
        return ranges;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public GeoDistanceAggregationBuilder unit(DistanceUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("[unit] must not be null: [" + name + "]");
        }
        this.unit = unit;
        return this;
    }

    public DistanceUnit unit() {
        return unit;
    }

    public GeoDistanceAggregationBuilder distanceType(GeoDistance distanceType) {
        if (distanceType == null) {
            throw new IllegalArgumentException("[distanceType] must not be null: [" + name + "]");
        }
        this.distanceType = distanceType;
        return this;
    }

    public GeoDistance distanceType() {
        return distanceType;
    }

    public GeoDistanceAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    public boolean keyed() {
        return keyed;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, ?> innerBuild(AggregationContext context,
            ValuesSourceConfig<ValuesSource.GeoPoint> config, AggregatorFactory<?> parent, Builder subFactoriesBuilder)
                    throws IOException {
        return new GeoDistanceRangeAggregatorFactory(name, type, config, origin, ranges, unit, distanceType, keyed, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(GeoDistanceParser.ORIGIN_FIELD.getPreferredName(), origin);
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        builder.field(GeoDistanceParser.UNIT_FIELD.getPreferredName(), unit);
        builder.field(GeoDistanceParser.DISTANCE_TYPE_FIELD.getPreferredName(), distanceType);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(origin, ranges, keyed, distanceType, unit);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        GeoDistanceAggregationBuilder other = (GeoDistanceAggregationBuilder) obj;
        return Objects.equals(origin, other.origin)
                && Objects.equals(ranges, other.ranges)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(distanceType, other.distanceType)
                && Objects.equals(unit, other.unit);
    }

}
