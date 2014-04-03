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
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Unmapped;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class GeoDistanceParser implements Aggregator.Parser {

    private static final ParseField ORIGIN_FIELD = new ParseField("origin", "center", "point", "por");

    @Override
    public String type() {
        return InternalGeoDistance.TYPE.name();
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
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser<ValuesSource.GeoPoint> vsParser = ValuesSourceParser.geoPoint(aggregationName, InternalGeoDistance.TYPE, context)
                .requiresSortedValues(true)
                .build();

        GeoPointParser geoPointParser = new GeoPointParser(aggregationName, InternalGeoDistance.TYPE, context, ORIGIN_FIELD);

        List<RangeAggregator.Range> ranges = null;
        DistanceUnit unit = DistanceUnit.DEFAULT;
        GeoDistance distanceType = GeoDistance.DEFAULT;
        boolean keyed = false;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (geoPointParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("unit".equals(currentFieldName)) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if ("distance_type".equals(currentFieldName) || "distanceType".equals(currentFieldName)) {
                    distanceType = GeoDistance.fromString(parser.text());
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ranges".equals(currentFieldName)) {
                    ranges = new ArrayList<>();
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
                                if ("from".equals(toOrFromOrKey)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    to = parser.doubleValue();
                                }
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                if ("key".equals(toOrFromOrKey)) {
                                    key = parser.text();
                                } else if ("from".equals(toOrFromOrKey)) {
                                    fromAsStr = parser.text();
                                } else if ("to".equals(toOrFromOrKey)) {
                                    toAsStr = parser.text();
                                }
                            }
                        }
                        ranges.add(new RangeAggregator.Range(key(key, from, to), from, fromAsStr, to, toAsStr));
                    }
                } else  {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in geo_distance aggregator [" + aggregationName + "]");
        }

        GeoPoint origin = geoPointParser.geoPoint();
        if (origin == null) {
            throw new SearchParseException(context, "Missing [origin] in geo_distance aggregator [" + aggregationName + "]");
        }

        return new GeoDistanceFactory(aggregationName, vsParser.config(), InternalGeoDistance.FACTORY, origin, unit, distanceType, ranges, keyed);
    }

    private static class GeoDistanceFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private final GeoPoint origin;
        private final DistanceUnit unit;
        private final GeoDistance distanceType;
        private final InternalRange.Factory rangeFactory;
        private final List<RangeAggregator.Range> ranges;
        private final boolean keyed;

        public GeoDistanceFactory(String name, ValuesSourceConfig<ValuesSource.GeoPoint> valueSourceConfig,
                                  InternalRange.Factory rangeFactory, GeoPoint origin, DistanceUnit unit, GeoDistance distanceType,
                                  List<RangeAggregator.Range> ranges, boolean keyed) {
            super(name, rangeFactory.type(), valueSourceConfig);
            this.origin = origin;
            this.unit = unit;
            this.distanceType = distanceType;
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new Unmapped(name, ranges, keyed, null, aggregationContext, parent, rangeFactory);
        }

        @Override
        protected Aggregator create(final ValuesSource.GeoPoint valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            final DistanceValues distanceValues = new DistanceValues(valuesSource, distanceType, origin, unit);
            ValuesSource.Numeric distanceSource = new DistanceSource(distanceValues, valuesSource.metaData());
            if (distanceSource.metaData().multiValued()) {
                // we need to ensure uniqueness
                distanceSource = new ValuesSource.Numeric.SortedAndUnique(distanceSource);
            }
            return new RangeAggregator(name, factories, distanceSource, null, rangeFactory, ranges, keyed, aggregationContext, parent);
        }

        private static class DistanceValues extends DoubleValues {

            private final ValuesSource.GeoPoint geoPointValues;
            private GeoPointValues geoValues;
            private final GeoDistance distanceType;
            private final GeoPoint origin;
            private final DistanceUnit unit;

            protected DistanceValues(ValuesSource.GeoPoint geoPointValues, GeoDistance distanceType, GeoPoint origin, DistanceUnit unit) {
                super(true);
                this.geoPointValues = geoPointValues;
                this.distanceType = distanceType;
                this.origin = origin;
                this.unit = unit;
            }

            @Override
            public int setDocument(int docId) {
                geoValues = geoPointValues.geoPointValues();
                return geoValues.setDocument(docId);
            }

            @Override
            public double nextValue() {
                final GeoPoint target = geoValues.nextValue();
                return distanceType.calculate(origin.getLat(), origin.getLon(), target.getLat(), target.getLon(), unit);
            }

        }

        private static class DistanceSource extends ValuesSource.Numeric {

            private final DoubleValues values;
            private final MetaData metaData;

            public DistanceSource(DoubleValues values, MetaData metaData) {
                this.values = values;
                // even if the geo points are unique, there's no guarantee the distances are
                this.metaData = MetaData.builder(metaData).uniqueness(MetaData.Uniqueness.UNKNOWN).build();
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

            @Override
            public boolean isFloatingPoint() {
                return true;
            }

            @Override
            public LongValues longValues() {
                throw new UnsupportedOperationException();
            }

            @Override
            public DoubleValues doubleValues() {
                return values;
            }

            @Override
            public BytesValues bytesValues() {
                throw new UnsupportedOperationException();
            }

        }

    }

}