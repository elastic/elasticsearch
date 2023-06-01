/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class IpRangeAggregationBuilder extends ValuesSourceAggregationBuilder<IpRangeAggregationBuilder> {
    public static final String NAME = "ip_range";
    public static final ValuesSourceRegistry.RegistryKey<IpRangeAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        IpRangeAggregatorSupplier.class
    );
    private static final ParseField MASK_FIELD = new ParseField("mask");

    public static final ObjectParser<IpRangeAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        IpRangeAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);

        PARSER.declareBoolean(IpRangeAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);

        PARSER.declareObjectArray((agg, ranges) -> {
            for (Range range : ranges)
                agg.addRange(range);
        }, (p, c) -> IpRangeAggregationBuilder.parseRange(p), RangeAggregator.RANGES_FIELD);
    }

    private static Range parseRange(XContentParser parser) throws IOException {
        String key = null;
        String from = null;
        String to = null;
        String mask = null;

        if (parser.currentToken() != Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[ranges] must contain objects, but hit a " + parser.currentToken());
        }
        while (parser.nextToken() != Token.END_OBJECT) {
            if (parser.currentToken() == Token.FIELD_NAME) {
                continue;
            }
            if (RangeAggregator.Range.KEY_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                key = parser.text();
            } else if (RangeAggregator.Range.FROM_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                from = parser.textOrNull();
            } else if (RangeAggregator.Range.TO_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                to = parser.textOrNull();
            } else if (MASK_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                mask = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected ip range parameter: [" + parser.currentName() + "]");
            }
        }
        if (mask != null) {
            if (key == null) {
                key = mask;
            }
            return new Range(key, mask);
        } else {
            return new Range(key, from, to);
        }
    }

    public static class Range implements ToXContentObject, Writeable {

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
            final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(mask);
            final InetAddress address = cidr.v1();
            final int prefixLength = cidr.v2();
            // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
            byte lower[] = address.getAddress();
            byte upper[] = address.getAddress();
            for (int i = prefixLength; i < 8 * lower.length; i++) {
                int m = 1 << (7 - (i & 7));
                lower[i >> 3] &= (byte) ~m;
                upper[i >> 3] |= (byte) m;
            }
            this.key = key;
            try {
                InetAddress fromAddress = InetAddress.getByAddress(lower);
                if (fromAddress.equals(InetAddressPoint.MIN_VALUE)) {
                    this.from = null;
                } else {
                    this.from = InetAddresses.toAddrString(fromAddress);
                }
                InetAddress inclusiveToAddress = InetAddress.getByAddress(upper);
                if (inclusiveToAddress.equals(InetAddressPoint.MAX_VALUE)) {
                    this.to = null;
                } else {
                    this.to = InetAddresses.toAddrString(InetAddressPoint.nextUp(inclusiveToAddress));
                }
            } catch (UnknownHostException bogus) {
                throw new AssertionError(bogus);
            }
        }

        private Range(StreamInput in) throws IOException {
            this.key = in.readOptionalString();
            this.from = in.readOptionalString();
            this.to = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
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
            return Objects.equals(key, that.key) && Objects.equals(from, that.from) && Objects.equals(to, that.to);
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

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        BinaryRangeAggregatorFactory.registerAggregators(builder);
    }

    private boolean keyed = false;
    private List<Range> ranges = new ArrayList<>();

    public IpRangeAggregationBuilder(String name) {
        super(name);
    }

    protected IpRangeAggregationBuilder(IpRangeAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.ranges = new ArrayList<>(clone.ranges);
        this.keyed = clone.keyed;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new IpRangeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
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
        super(in);
        final int numRanges = in.readVInt();
        for (int i = 0; i < numRanges; ++i) {
            addRange(new Range(in));
        }
        keyed = in.readBoolean();
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.IP;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(ranges);
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
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        IpRangeAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);

        List<BinaryRangeAggregator.Range> ranges = new ArrayList<>();
        if (this.ranges.size() == 0) {
            throw new IllegalArgumentException("No [ranges] specified for the [" + this.getName() + "] aggregation");
        }
        for (Range range : this.ranges) {
            ranges.add(new BinaryRangeAggregator.Range(range.key, toBytesRef(range.from), toBytesRef(range.to)));
        }

        return new BinaryRangeAggregatorFactory(
            name,
            config,
            ranges,
            keyed,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, ranges);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        IpRangeAggregationBuilder that = (IpRangeAggregationBuilder) obj;
        return keyed == that.keyed && ranges.equals(that.ranges);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
