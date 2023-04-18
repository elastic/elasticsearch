/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * A builder for IP prefix aggregations. This builder can operate with both IPv4 and IPv6 fields.
 */
public class IpPrefixAggregationBuilder extends ValuesSourceAggregationBuilder<IpPrefixAggregationBuilder> {
    public static final String NAME = "ip_prefix";
    public static final ValuesSourceRegistry.RegistryKey<IpPrefixAggregationSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        IpPrefixAggregationSupplier.class
    );
    public static final ObjectParser<IpPrefixAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        IpPrefixAggregationBuilder::new
    );

    public static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    public static final ParseField IS_IPV6_FIELD = new ParseField("is_ipv6");
    public static final ParseField APPEND_PREFIX_LENGTH_FIELD = new ParseField("append_prefix_length");
    public static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
        PARSER.declareInt(IpPrefixAggregationBuilder::prefixLength, PREFIX_LENGTH_FIELD);
        PARSER.declareBoolean(IpPrefixAggregationBuilder::isIpv6, IS_IPV6_FIELD);
        PARSER.declareLong(IpPrefixAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD);
        PARSER.declareBoolean(IpPrefixAggregationBuilder::appendPrefixLength, APPEND_PREFIX_LENGTH_FIELD);
        PARSER.declareBoolean(IpPrefixAggregationBuilder::keyed, KEYED_FIELD);
    }

    private static final int IPV6_MAX_PREFIX_LENGTH = 128;
    private static final int IPV4_MAX_PREFIX_LENGTH = 32;
    private static final int MIN_PREFIX_LENGTH = 0;

    /** Read from a stream, for internal use only. */
    public IpPrefixAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.prefixLength = in.readVInt();
        this.isIpv6 = in.readBoolean();
        this.minDocCount = in.readVLong();
        this.appendPrefixLength = in.readBoolean();
        this.keyed = in.readBoolean();
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        IpPrefixAggregatorFactory.registerAggregators(builder);
    }

    private long minDocCount = 1;
    private int prefixLength = -1;
    private boolean isIpv6 = false;
    private boolean appendPrefixLength = false;
    private boolean keyed = false;

    private static <T> void throwOnInvalidFieldValue(final String fieldName, final T minValue, final T maxValue, final T fieldValue) {
        throw new IllegalArgumentException(
            "["
                + fieldName
                + "] must be in range ["
                + minValue.toString()
                + ", "
                + maxValue.toString()
                + "] while value is ["
                + fieldValue.toString()
                + "]"
        );
    }

    /** Set the minDocCount on this builder, and return the builder so that calls can be chained. */
    public IpPrefixAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 1) {
            throwOnInvalidFieldValue(MIN_DOC_COUNT_FIELD.getPreferredName(), 1, Integer.MAX_VALUE, minDocCount);
        }
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Set the prefixLength on this builder, and return the builder so that calls can be chained.
     *
     * @throws IllegalArgumentException if prefixLength is negative.
     * */
    public IpPrefixAggregationBuilder prefixLength(int prefixLength) {
        if (prefixLength < MIN_PREFIX_LENGTH) {
            throwOnInvalidFieldValue(
                PREFIX_LENGTH_FIELD.getPreferredName(),
                0,
                isIpv6 ? IPV6_MAX_PREFIX_LENGTH : IPV4_MAX_PREFIX_LENGTH,
                prefixLength
            );
        }
        this.prefixLength = prefixLength;
        return this;
    }

    /** Set the isIpv6 on this builder, and return the builder so that calls can be chained. */
    public IpPrefixAggregationBuilder isIpv6(boolean isIpv6) {
        this.isIpv6 = isIpv6;
        return this;
    }

    /** Set the appendPrefixLength on this builder, and return the builder so that calls can be chained. */
    public IpPrefixAggregationBuilder appendPrefixLength(boolean appendPrefixLength) {
        this.appendPrefixLength = appendPrefixLength;
        return this;
    }

    /** Set the keyed on this builder, and return the builder so that calls can be chained. */
    public IpPrefixAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /** Create a new builder with the given name. */
    public IpPrefixAggregationBuilder(String name) {
        super(name);
    }

    protected IpPrefixAggregationBuilder(
        IpPrefixAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.minDocCount = clone.minDocCount;
        this.isIpv6 = clone.isIpv6;
        this.prefixLength = clone.prefixLength;
        this.appendPrefixLength = clone.appendPrefixLength;
        this.keyed = clone.keyed;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new IpPrefixAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(prefixLength);
        out.writeBoolean(isIpv6);
        out.writeVLong(minDocCount);
        out.writeBoolean(appendPrefixLength);
        out.writeBoolean(keyed);
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.IP;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        IpPrefixAggregationSupplier aggregationSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);

        if (prefixLength < 0
            || (isIpv6 == false && prefixLength > IPV4_MAX_PREFIX_LENGTH)
            || (isIpv6 && prefixLength > IPV6_MAX_PREFIX_LENGTH)) {
            throwOnInvalidFieldValue(
                PREFIX_LENGTH_FIELD.getPreferredName(),
                MIN_PREFIX_LENGTH,
                isIpv6 ? IPV6_MAX_PREFIX_LENGTH : IPV4_MAX_PREFIX_LENGTH,
                prefixLength
            );
        }

        IpPrefixAggregator.IpPrefix ipPrefix = new IpPrefixAggregator.IpPrefix(
            isIpv6,
            prefixLength,
            appendPrefixLength,
            extractNetmask(prefixLength, isIpv6)
        );

        return new IpPrefixAggregatorFactory(
            name,
            config,
            keyed,
            minDocCount,
            ipPrefix,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregationSupplier
        );
    }

    /**
     * @param prefixLength the network prefix length which defines the size of the network.
     * @param isIpv6 true for an IPv6 netmask, false for an IPv4 netmask.
     *
     * @return a 16-bytes representation of the subnet with 1s identifying the network
     *         part and 0s identifying the host part.
     *
     * @throws IllegalArgumentException if prefixLength is not in range [0, 128] for an IPv6
     *         network, or is not in range [0, 32] for an IPv4 network.
     */
    public static BytesRef extractNetmask(int prefixLength, boolean isIpv6) {
        if (prefixLength < 0
            || (isIpv6 == false && prefixLength > IPV4_MAX_PREFIX_LENGTH)
            || (isIpv6 && prefixLength > IPV6_MAX_PREFIX_LENGTH)) {
            throwOnInvalidFieldValue(
                PREFIX_LENGTH_FIELD.getPreferredName(),
                MIN_PREFIX_LENGTH,
                isIpv6 ? IPV6_MAX_PREFIX_LENGTH : IPV4_MAX_PREFIX_LENGTH,
                prefixLength
            );
        }

        byte[] ipv4Address = { 0, 0, 0, 0 };
        byte[] ipv6Address = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        byte[] ipAddress = (isIpv6) ? ipv6Address : ipv4Address;
        int bytesCount = prefixLength / 8;
        int bitsCount = prefixLength % 8;
        int i = 0;
        // NOTE: first set whole bytes to 255 (0xFF)
        for (; i < bytesCount; i++) {
            ipAddress[i] = (byte) 0xFF;
        }
        // NOTE: then set the remaining bits to 1.
        // Trailing bits are already set to 0 at initialization time.
        // Example: for prefixLength = 20, we first set 16 bits (2 bytes)
        // to 0xFF, then set the remaining 4 bits to 1.
        if (bitsCount > 0) {
            int rem = 0;
            for (int j = 0; j < bitsCount; j++) {
                rem |= 1 << (7 - j);
            }
            ipAddress[i] = (byte) rem;
        }

        try {
            return new BytesRef(InetAddress.getByAddress(ipAddress).getAddress());
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Unable to get the ip address for [" + Arrays.toString(ipAddress) + "]", e);
        }
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
        builder.field(IS_IPV6_FIELD.getPreferredName(), isIpv6);
        builder.field(APPEND_PREFIX_LENGTH_FIELD.getPreferredName(), appendPrefixLength);
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        builder.field(MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        IpPrefixAggregationBuilder that = (IpPrefixAggregationBuilder) o;
        return minDocCount == that.minDocCount && prefixLength == that.prefixLength && isIpv6 == that.isIpv6;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minDocCount, prefixLength, isIpv6);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_1_0;
    }
}
