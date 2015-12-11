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
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IPv4RangeAggregatorFactory extends Factory {

    public IPv4RangeAggregatorFactory(String name, List<Range> ranges) {
        super(name, InternalIPv4Range.FACTORY, ranges);
    }

    @Override
    public String getWriteableName() {
        return InternalIPv4Range.TYPE.name();
    }

    @Override
    protected IPv4RangeAggregatorFactory createFactoryFromStream(String name, StreamInput in) throws IOException {
        int size = in.readVInt();
        List<Range> ranges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ranges.add(Range.PROTOTYPE.readFrom(in));
        }
        return new IPv4RangeAggregatorFactory(name, ranges);
    }

    public static class Range extends RangeAggregator.Range {

        static final Range PROTOTYPE = new Range(null, -1, null, -1, null, null);
        static final ParseField MASK_FIELD = new ParseField("mask");

        private String cidr;

        public Range(String key, double from, double to) {
            super(key, from, to);
        }

        public Range(String key, String from, String to) {
            super(key, from, to);
        }

        public Range(String key, String cidr) {
            super(key, -1, null, -1, null);
            this.cidr = cidr;
            if (cidr != null) {
                parseMaskRange();
            }
        }

        private Range(String key, double from, String fromAsStr, double to, String toAsStr, String cidr) {
            super(key, from, fromAsStr, to, toAsStr);
            this.cidr = cidr;
            if (cidr != null) {
                parseMaskRange();
            }
        }

        public String mask() {
            return cidr;
        }

        private void parseMaskRange() throws IllegalArgumentException {
            long[] fromTo = Cidrs.cidrMaskToMinMax(cidr);
            from = fromTo[0] == 0 ? Double.NEGATIVE_INFINITY : fromTo[0];
            to = fromTo[1] == InternalIPv4Range.MAX_IP ? Double.POSITIVE_INFINITY : fromTo[1];
            if (key == null) {
                key = cidr;
            }
        }

        @Override
        public Range fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {

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
        public Range readFrom(StreamInput in) throws IOException {
            String key = in.readOptionalString();
            String fromAsStr = in.readOptionalString();
            String toAsStr = in.readOptionalString();
            double from = in.readDouble();
            double to = in.readDouble();
            String mask = in.readOptionalString();
            return new Range(key, from, fromAsStr, to, toAsStr, mask);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(fromAsStr);
            out.writeOptionalString(toAsStr);
            out.writeDouble(from);
            out.writeDouble(to);
            out.writeOptionalString(cidr);
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
