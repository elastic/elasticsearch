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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoDistance.FixedSourceDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Unmapped;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.GeoPointValuesSourceParser;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.GeoPointParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class GeoDistanceParser extends GeoPointValuesSourceParser {

    private static final ParseField ORIGIN_FIELD = new ParseField("origin", "center", "point", "por");
    private static final ParseField UNIT_FIELD = new ParseField("unit");
    private static final ParseField DISTANCE_TYPE_FIELD = new ParseField("distance_type");

    private GeoPointParser geoPointParser = new GeoPointParser(InternalGeoDistance.TYPE, ORIGIN_FIELD);

    public GeoDistanceParser() {
        super(true, false);
    }

    @Override
    public String type() {
        return InternalGeoDistance.TYPE.name();
    }

    public static class Range extends RangeAggregator.Range {

        static final Range PROTOTYPE = new Range(null, -1, -1);

        public Range(String key, double from, double to) {
            super(key(key, from, to), from, to);
        }

        private static String key(String key, double from, double to) {
            if (key != null) {
                return key;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(from == 0 ? "*" : from);
            sb.append("-");
            sb.append(Double.isInfinite(to) ? "*" : to);
            return sb.toString();
        }

        @Override
        public Range readFrom(StreamInput in) throws IOException {
            String key = in.readOptionalString();
            double from = in.readDouble();
            double to = in.readDouble();
            return new Range(key, from, to);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeDouble(from);
            out.writeDouble(to);
        }

    }

    @Override
    protected ValuesSourceAggregatorFactory<org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint> createFactory(
            String aggregationName, ValuesSourceType valuesSourceType, ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        GeoPoint origin = (GeoPoint) otherOptions.get(ORIGIN_FIELD);
        List<Range> ranges = (List<Range>) otherOptions.get(RangeAggregator.RANGES_FIELD);
        GeoDistanceFactory factory = new GeoDistanceFactory(aggregationName, origin, ranges);
        Boolean keyed = (Boolean) otherOptions.get(RangeAggregator.KEYED_FIELD);
        if (keyed != null) {
            factory.keyed(keyed);
        }
        DistanceUnit unit = (DistanceUnit) otherOptions.get(UNIT_FIELD);
        if (unit != null) {
            factory.unit(unit);
        }
        GeoDistance distanceType = (GeoDistance) otherOptions.get(DISTANCE_TYPE_FIELD);
        if (distanceType != null) {
            factory.distanceType(distanceType);
        }
        return factory;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (geoPointParser.token(aggregationName, currentFieldName, token, parser, parseFieldMatcher, otherOptions)) {
            return true;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            if (parseFieldMatcher.match(currentFieldName, UNIT_FIELD)) {
                DistanceUnit unit = DistanceUnit.fromString(parser.text());
                otherOptions.put(UNIT_FIELD, unit);
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, DISTANCE_TYPE_FIELD)) {
                GeoDistance distanceType = GeoDistance.fromString(parser.text());
                otherOptions.put(DISTANCE_TYPE_FIELD, distanceType);
                return true;
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parseFieldMatcher.match(currentFieldName, RangeAggregator.KEYED_FIELD)) {
                boolean keyed = parser.booleanValue();
                otherOptions.put(RangeAggregator.KEYED_FIELD, keyed);
                return true;
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            if (parseFieldMatcher.match(currentFieldName, RangeAggregator.RANGES_FIELD)) {
                List<Range> ranges = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    String fromAsStr = null;
                    String toAsStr = null;
                    double from = 0.0;
                    double to = Double.POSITIVE_INFINITY;
                    String key = null;
                    String toOrFromOrKey = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            toOrFromOrKey = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if (parseFieldMatcher.match(toOrFromOrKey, Range.FROM_FIELD)) {
                                from = parser.doubleValue();
                            } else if (parseFieldMatcher.match(toOrFromOrKey, Range.TO_FIELD)) {
                                to = parser.doubleValue();
                            }
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if (parseFieldMatcher.match(toOrFromOrKey, Range.KEY_FIELD)) {
                                key = parser.text();
                            } else if (parseFieldMatcher.match(toOrFromOrKey, Range.FROM_FIELD)) {
                                fromAsStr = parser.text();
                            } else if (parseFieldMatcher.match(toOrFromOrKey, Range.TO_FIELD)) {
                                toAsStr = parser.text();
                            }
                        }
                    }
                    if (fromAsStr != null || toAsStr != null) {
                        ranges.add(new Range(key, Double.parseDouble(fromAsStr), Double.parseDouble(toAsStr)));
                    } else {
                        ranges.add(new Range(key, from, to));
                    }
                }
                otherOptions.put(RangeAggregator.RANGES_FIELD, ranges);
                return true;
            }
        }
        return false;
    }

    public static class GeoDistanceFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private final GeoPoint origin;
        private final InternalRange.Factory rangeFactory;
        private final List<Range> ranges;
        private DistanceUnit unit = DistanceUnit.DEFAULT;
        private GeoDistance distanceType = GeoDistance.DEFAULT;
        private boolean keyed = false;

        public GeoDistanceFactory(String name, GeoPoint origin, List<Range> ranges) {
            this(name, origin, InternalGeoDistance.FACTORY, ranges);
        }

        private GeoDistanceFactory(String name, GeoPoint origin, InternalRange.Factory rangeFactory, List<Range> ranges) {
            super(name, rangeFactory.type(), rangeFactory.getValueSourceType(), rangeFactory.getValueType());
            this.origin = origin;
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
        }

        @Override
        public String getWriteableName() {
            return InternalGeoDistance.TYPE.name();
        }

        public void unit(DistanceUnit unit) {
            this.unit = unit;
        }

        public DistanceUnit unit() {
            return unit;
        }

        public void distanceType(GeoDistance distanceType) {
            this.distanceType = distanceType;
        }

        public GeoDistance distanceType() {
            return distanceType;
        }

        public void keyed(boolean keyed) {
            this.keyed = keyed;
        }

        public boolean keyed() {
            return keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            return new Unmapped(name, ranges, keyed, config.format(), aggregationContext, parent, rangeFactory, pipelineAggregators,
                    metaData);
        }

        @Override
        protected Aggregator doCreateInternal(final ValuesSource.GeoPoint valuesSource, AggregationContext aggregationContext,
                Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData)
                throws IOException {
            DistanceSource distanceSource = new DistanceSource(valuesSource, distanceType, origin, unit);
            return new RangeAggregator(name, factories, distanceSource, config.format(), rangeFactory, ranges, keyed, aggregationContext,
                    parent,
                    pipelineAggregators, metaData);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(ORIGIN_FIELD.getPreferredName(), origin);
            builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
            builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
            builder.field(UNIT_FIELD.getPreferredName(), unit);
            builder.field(DISTANCE_TYPE_FIELD.getPreferredName(), distanceType);
            return builder;
        }

        @Override
        protected ValuesSourceAggregatorFactory<org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint> innerReadFrom(
                String name, ValuesSourceType valuesSourceType, ValueType targetValueType, StreamInput in) throws IOException {
            GeoPoint origin = new GeoPoint(in.readDouble(), in.readDouble());
            int size = in.readVInt();
            List<Range> ranges = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                ranges.add(Range.PROTOTYPE.readFrom(in));
            }
            GeoDistanceFactory factory = new GeoDistanceFactory(name, origin, ranges);
            factory.keyed = in.readBoolean();
            factory.distanceType = GeoDistance.readGeoDistanceFrom(in);
            factory.unit = DistanceUnit.readDistanceUnit(in);
            return factory;
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
            DistanceUnit.writeDistanceUnit(out, unit);
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(origin, ranges, keyed, distanceType, unit);
        }

        @Override
        protected boolean innerEquals(Object obj) {
            GeoDistanceFactory other = (GeoDistanceFactory) obj;
            return Objects.equals(origin, other.origin)
                    && Objects.equals(ranges, other.ranges)
                    && Objects.equals(keyed, other.keyed)
                    && Objects.equals(distanceType, other.distanceType)
                    && Objects.equals(unit, other.unit);
        }

        private static class DistanceSource extends ValuesSource.Numeric {

            private final ValuesSource.GeoPoint source;
            private final GeoDistance distanceType;
            private final DistanceUnit unit;
            private final org.elasticsearch.common.geo.GeoPoint origin;

            public DistanceSource(ValuesSource.GeoPoint source, GeoDistance distanceType, org.elasticsearch.common.geo.GeoPoint origin, DistanceUnit unit) {
                this.source = source;
                // even if the geo points are unique, there's no guarantee the distances are
                this.distanceType = distanceType;
                this.unit = unit;
                this.origin = origin;
            }

            @Override
            public boolean isFloatingPoint() {
                return true;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext ctx) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
                final MultiGeoPointValues geoValues = source.geoPointValues(ctx);
                final FixedSourceDistance distance = distanceType.fixedSourceDistance(origin.getLat(), origin.getLon(), unit);
                return GeoDistance.distanceValues(geoValues, distance);
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
                throw new UnsupportedOperationException();
            }

        }

    }

    @Override
    public AggregatorFactory[] getFactoryPrototypes() {
        return new AggregatorFactory[] { new GeoDistanceFactory(null, null, Collections.emptyList()) };
    }

}