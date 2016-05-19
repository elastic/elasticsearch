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
package org.elasticsearch.search.aggregations.bucket.range.ip;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.BinaryRangeAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;


public final class IpRangeAggregationBuilder
        extends ValuesSourceAggregationBuilder<ValuesSource.Bytes, IpRangeAggregationBuilder> {
    private static final String NAME = "ip_range";
    public static final ParseField AGGREGATION_NAME_FIELD = new ParseField(NAME);
    private static final InternalAggregation.Type TYPE = new InternalAggregation.Type(NAME);

    public static class Range implements ToXContent {
        private final String key;
        private final String from;
        private final String to;

        Range(String key, String from, String to) {
            if (from != null) {
                InetAddresses.forString(from);
            }
            if (to != null) {
                InetAddresses.forString(to);
            }
            this.key = key;
            this.from = from;
            this.to = to;
        }

        Range(String key, String mask) {
            String[] splits = mask.split("/");
            if (splits.length != 2) {
                throw new IllegalArgumentException("Expected [ip/prefix_length] but got [" + mask
                        + "], which contains zero or more than one [/]");
            }
            InetAddress value = InetAddresses.forString(splits[0]);
            int prefixLength = Integer.parseInt(splits[1]);
            // copied from InetAddressPoint.newPrefixQuery
            if (prefixLength < 0 || prefixLength > 8 * value.getAddress().length) {
                throw new IllegalArgumentException("illegal prefixLength [" + prefixLength
                        + "] in [" + mask + "]. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges");
            }
            // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
            byte lower[] = value.getAddress();
            byte upper[] = value.getAddress();
            for (int i = prefixLength; i < 8 * lower.length; i++) {
                int m = 1 << (7 - (i & 7));
                lower[i >> 3] &= ~m;
                upper[i >> 3] |= m;
            }
            this.key = key;
            try {
                this.from = InetAddresses.toAddrString(InetAddress.getByAddress(lower));
                this.to = InetAddresses.toAddrString(InetAddress.getByAddress(upper));
            } catch (UnknownHostException bogus) {
                throw new AssertionError(bogus);
            }
        }

        private Range(StreamInput in) throws IOException {
            this.key = in.readOptionalString();
            this.from = in.readOptionalString();
            this.to = in.readOptionalString();
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(from);
            out.writeOptionalString(to);
        }

        public String getKey() {
            return key;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Range that = (Range) obj;
            return Objects.equals(key, that.key)
                    && Objects.equals(from, that.from)
                    && Objects.equals(to, that.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, from, to);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(RangeAggregator.Range.KEY_FIELD.getPreferredName(), key);
            }
            if (from != null) {
                builder.field(RangeAggregator.Range.FROM_FIELD.getPreferredName(), from);
            }
            if (to != null) {
                builder.field(RangeAggregator.Range.TO_FIELD.getPreferredName(), to);
            }
            builder.endObject();
            return builder;
        }
    }

    private boolean keyed = false;
    private List<Range> ranges = new ArrayList<>();

    public IpRangeAggregationBuilder(String name) {
        super(name, TYPE, ValuesSourceType.BYTES, ValueType.IP);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public IpRangeAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    public boolean keyed() {
        return keyed;
    }

    /** Get the current list or ranges that are configured on this aggregation. */
    public List<Range> getRanges() {
        return Collections.unmodifiableList(ranges);
    }

    /** Add a new {@link Range} to this aggregation. */
    public IpRangeAggregationBuilder addRange(Range range) {
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
    public IpRangeAggregationBuilder addRange(String key, String from, String to) {
        addRange(new Range(key, from, to));
        return this;
    }

    /**
     * Add a new range to this aggregation using the CIDR notation.
     */
    public IpRangeAggregationBuilder addMaskRange(String key, String mask) {
        return addRange(new Range(key, mask));
    }

    /**
     * Same as {@link #addMaskRange(String, String)} but uses the mask itself as
     * a key.
     */
    public IpRangeAggregationBuilder addMaskRange(String mask) {
        return addRange(new Range(mask, mask));
    }

    /**
     * Same as {@link #addRange(String, String, String)} but the key will be
     * automatically generated.
     */
    public IpRangeAggregationBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no
     * lower bound.
     */
    public IpRangeAggregationBuilder addUnboundedTo(String key, String to) {
        addRange(new Range(key, null, to));
        return this;
    }

    /**
     * Same as {@link #addUnboundedTo(String, String)} but the key will be
     * generated automatically.
     */
    public IpRangeAggregationBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    /**
     * Same as {@link #addRange(String, String, String)} but there will be no
     * upper bound.
     */
    public IpRangeAggregationBuilder addUnboundedFrom(String key, String from) {
        addRange(new Range(key, from, null));
        return this;
    }

    @Override
    public IpRangeAggregationBuilder script(Script script) {
        throw new IllegalArgumentException("[ip_range] does not support scripts");
    }

    /**
     * Same as {@link #addUnboundedFrom(String, String)} but the key will be
     * generated automatically.
     */
    public IpRangeAggregationBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

    public IpRangeAggregationBuilder(StreamInput in) throws IOException {
        super(in, TYPE, ValuesSourceType.BYTES, ValueType.IP);
        final int numRanges = in.readVInt();
        for (int i = 0; i < numRanges; ++i) {
            addRange(new Range(in));
        }
        keyed = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(ranges.size());
        for (Range range : ranges) {
            range.writeTo(out);
        }
        out.writeBoolean(keyed);
    }

    private static BytesRef toBytesRef(String ip) {
        if (ip == null) {
            return null;
        }
        InetAddress address = InetAddresses.forString(ip);
        byte[] bytes = InetAddressPoint.encode(address);
        return new BytesRef(bytes);
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.Bytes, ?> innerBuild(
            AggregationContext context, ValuesSourceConfig<ValuesSource.Bytes> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder)
                    throws IOException {
        List<BinaryRangeAggregator.Range> ranges = new ArrayList<>();
        for (Range range : this.ranges) {
            ranges.add(new BinaryRangeAggregator.Range(range.key, toBytesRef(range.from), toBytesRef(range.to)));
        }
        return new BinaryRangeAggregatorFactory(name, TYPE, config, ranges,
                keyed, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(keyed, ranges);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        IpRangeAggregationBuilder that = (IpRangeAggregationBuilder) obj;
        return keyed == that.keyed
                && ranges.equals(that.ranges);
    }
}
