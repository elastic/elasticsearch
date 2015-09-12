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
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoDistance.FixedSourceDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Unmapped;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.GeoPointParser;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        ValuesSourceParser<ValuesSource.GeoPoint> vsParser = ValuesSourceParser.geoPoint(aggregationName, InternalGeoDistance.TYPE, context).build();

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
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
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
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "]: ["
                        + currentFieldName + "].", parser.getTokenLocation());
            }
        }

        if (ranges == null) {
            throw new SearchParseException(context, "Missing [ranges] in geo_distance aggregator [" + aggregationName + "]",
                    parser.getTokenLocation());
        }

        GeoPoint origin = geoPointParser.geoPoint();
        if (origin == null) {
            throw new SearchParseException(context, "Missing [origin] in geo_distance aggregator [" + aggregationName + "]",
                    parser.getTokenLocation());
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

}