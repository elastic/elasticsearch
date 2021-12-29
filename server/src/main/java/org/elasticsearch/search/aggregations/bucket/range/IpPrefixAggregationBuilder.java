/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.BytesRef;
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
import java.util.Map;
import java.util.Objects;

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

    public static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_len");
    public static final ParseField IS_IPV6_FIELD = new ParseField("is_ipv6");
    public static final ParseField APPEND_PREFIX_LENGTH_FIELD = new ParseField("append_prefix_len");
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

    private long minDocCount = 0;
    private int prefixLength = -1;
    private boolean isIpv6 = false;
    private boolean appendPrefixLength = false;
    private boolean keyed = false;

    public IpPrefixAggregationBuilder minDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public IpPrefixAggregationBuilder prefixLength(int prefixLength) {
        if (prefixLength < MIN_PREFIX_LENGTH) {
            throw new IllegalArgumentException("[prefix_len] must not be less than " + MIN_PREFIX_LENGTH + ": [" + name + "]");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    public IpPrefixAggregationBuilder isIpv6(boolean isIpv6) {
        this.isIpv6 = isIpv6;
        return this;
    }

    public IpPrefixAggregationBuilder appendPrefixLength(boolean appendPrefixLength) {
        this.appendPrefixLength = appendPrefixLength;
        return this;
    }

    public IpPrefixAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

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

        if (isIpv6 && prefixLength > IPV6_MAX_PREFIX_LENGTH) {
            throw new IllegalArgumentException(
                "["
                    + PREFIX_LENGTH_FIELD.getPreferredName()
                    + "] must be in range ["
                    + MIN_PREFIX_LENGTH
                    + ", "
                    + IPV6_MAX_PREFIX_LENGTH
                    + "] for aggregation ["
                    + this.getName()
                    + "]"
            );
        }

        if (!isIpv6 && prefixLength > IPV4_MAX_PREFIX_LENGTH) {
            throw new IllegalArgumentException(
                "["
                    + PREFIX_LENGTH_FIELD.getPreferredName()
                    + "] must be in range ["
                    + MIN_PREFIX_LENGTH
                    + ", "
                    + IPV4_MAX_PREFIX_LENGTH
                    + "] for aggregation ["
                    + this.getName()
                    + "]"
            );
        }

        byte[] subnet = extractSubnet(prefixLength, isIpv6);
        if (subnet == null) {
            throw new IllegalArgumentException(
                "["
                    + PREFIX_LENGTH_FIELD.getPreferredName()
                    + "] must be in range ["
                    + MIN_PREFIX_LENGTH
                    + ", "
                    + IPV4_MAX_PREFIX_LENGTH
                    + "] for aggregation ["
                    + this.getName()
                    + "]"
            );
        }
        IpPrefixAggregator.IpPrefix ipPrefix = new IpPrefixAggregator.IpPrefix(
            isIpv6,
            prefixLength,
            appendPrefixLength,
            new BytesRef(subnet)
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

    private static byte[] extractSubnet(int prefixLength, boolean isIpv6) {
        if (prefixLength < 0 || (!isIpv6 && prefixLength > 32) || (isIpv6 && prefixLength > 128)) {
            return null;
        }

        byte[] ipv4Address = { 0, 0, 0, 0 };
        byte[] ipv6Address = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        byte[] ipAddress = (isIpv6) ? ipv6Address : ipv4Address;
        int bytesCount = prefixLength / 8;
        int bitsCount = prefixLength % 8;
        int i = 0;
        for (; i < bytesCount; i++) {
            ipAddress[i] = (byte) 0xFF;
        }
        if (bitsCount > 0) {
            int rem = 0;
            for (int j = 0; j < bitsCount; j++) {
                rem |= 1 << (7 - j);
            }
            ipAddress[i] = (byte) rem;
        }

        try {
            return InetAddress.getByAddress(ipAddress).getAddress();
        } catch (UnknownHostException e) {
            return null;
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
        if (!super.equals(o)) return false;
        IpPrefixAggregationBuilder that = (IpPrefixAggregationBuilder) o;
        return minDocCount == that.minDocCount && prefixLength == that.prefixLength && isIpv6 == that.isIpv6;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minDocCount, prefixLength, isIpv6);
    }
}
