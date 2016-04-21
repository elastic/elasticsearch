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

package org.elasticsearch.search.aggregations.bucket.range.ipv4;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.Cidrs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Objects;

public class IPv4RangeAggregatorBuilder extends AbstractRangeBuilder<IPv4RangeAggregatorBuilder, IPv4RangeAggregatorBuilder.Range> {
    public static final String NAME = InternalIPv4Range.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);

    public IPv4RangeAggregatorBuilder(String name) {
        super(name, InternalIPv4Range.FACTORY);
    }

    /**
     * Read from a stream.
     */
    public IPv4RangeAggregatorBuilder(StreamInput in) throws IOException {
        super(in, InternalIPv4Range.FACTORY, Range::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
    public IPv4RangeAggregatorBuilder addRange(String key, String from, String to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Same as {@link #addMaskRange(String, String)} but uses the mask itself as
     * a key.
     */
    public IPv4RangeAggregatorBuilder addMaskRange(String key, String mask) {
        return addRange(new Range(key, mask));
    }

    /**
     * Same as {@link #addMaskRange(String, String)} but uses the mask itself as
     * a key.
     */
    public IPv4RangeAggregatorBuilder addMaskRange(String mask) {
        return addRange(new Range(mask, mask));
    }

    /**
     * Same as {@link #addRange(String, String, String)} but the key will be
     * automatically generated.
     */
    public IPv4RangeAggregatorBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no
     * lower bound.
     */
    public IPv4RangeAggregatorBuilder addUnboundedTo(String key, String to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, String)} but the key will be
     * generated automatically.
     */
    public IPv4RangeAggregatorBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no
     * upper bound.
     */
    public IPv4RangeAggregatorBuilder addUnboundedFrom(String key, String from) {
        addRange(new Range(key, from, null));
        return this;
    }

    /**
     * Same as {@link #addUnboundedFrom(String, String)} but the key will be
     * generated automatically.
     */
    public IPv4RangeAggregatorBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

    @Override
    protected Ipv4RangeAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new Ipv4RangeAggregatorFactory(name, type, config, ranges, keyed, rangeFactory, context, parent, subFactoriesBuilder,
                metaData);
    }

    public static class Range extends RangeAggregator.Range {
        static final ParseField MASK_FIELD = new ParseField("mask");

        private final String cidr;

        public Range(String key, Double from, Double to) {
            this(key, from, null, to, null, null);
        }

        public Range(String key, String from, String to) {
            this(key, null, from, null, to, null);
        }

        public Range(String key, String cidr) {
            this(key, null, null, null, null, cidr);
        }

        private Range(String key, Double from, String fromAsStr, Double to, String toAsStr, String cidr) {
            super(key, from, fromAsStr, to, toAsStr);
            this.cidr = cidr;
        }

        /**
         * Read from a stream.
         */
        public Range(StreamInput in) throws IOException {
            super(in);
            cidr = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(cidr);
        }

        public String mask() {
            return cidr;
        }

        @Override
        public Range process(DocValueFormat parser, SearchContext context) {
            assert parser != null;
            Double from = this.from;
            Double to = this.to;
            String key = this.key;
            if (fromAsStr != null) {
                from = parser.parseDouble(fromAsStr, false, context.nowCallable());
            }
            if (toAsStr != null) {
                to = parser.parseDouble(toAsStr, false, context.nowCallable());
            }
            if (cidr != null) {
                long[] fromTo = Cidrs.cidrMaskToMinMax(cidr);
                from = fromTo[0] == 0 ? Double.NEGATIVE_INFINITY : fromTo[0];
                to = fromTo[1] == InternalIPv4Range.MAX_IP ? Double.POSITIVE_INFINITY : fromTo[1];
                if (this.key == null) {
                    key = cidr;
                }
            }
            return new Range(key, from, to);
        }

        public static Range fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
            XContentParser.Token token;
            String currentFieldName = null;
            double from = Double.NEGATIVE_INFINITY;
            String fromAsStr = null;
            double to = Double.POSITIVE_INFINITY;
            String toAsStr = null;
            String key = null;
            String cidr = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (parseFieldMatcher.match(currentFieldName, FROM_FIELD)) {
                        from = parser.doubleValue();
                    } else if (parseFieldMatcher.match(currentFieldName, TO_FIELD)) {
                        to = parser.doubleValue();
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (parseFieldMatcher.match(currentFieldName, FROM_FIELD)) {
                        fromAsStr = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, TO_FIELD)) {
                        toAsStr = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, KEY_FIELD)) {
                        key = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, MASK_FIELD)) {
                        cidr = parser.text();
                    }
                }
            }
            return new Range(key, from, fromAsStr, to, toAsStr, cidr);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            if (cidr != null) {
                builder.field(MASK_FIELD.getPreferredName(), cidr);
            } else {
                if (Double.isFinite(from)) {
                    builder.field(FROM_FIELD.getPreferredName(), from);
                }
                if (Double.isFinite(to)) {
                    builder.field(TO_FIELD.getPreferredName(), to);
                }
                if (fromAsStr != null) {
                    builder.field(FROM_FIELD.getPreferredName(), fromAsStr);
                }
                if (toAsStr != null) {
                    builder.field(TO_FIELD.getPreferredName(), toAsStr);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), cidr);
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj)
                    && Objects.equals(cidr, ((Range) obj).cidr);
        }

    }

}
