/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range.KEY_FIELD;

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

    private static final ParseField IS_IPV6 = new ParseField("is_ipv6");
    private static final ParseField PREFIX_LENGTH = new ParseField("prefix_len");

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, false, false);
        PARSER.declareBoolean(IpPrefixAggregationBuilder::keyed, RangeAggregator.KEYED_FIELD);
        PARSER.declareLong(IpPrefixAggregationBuilder::minDocCount, Histogram.MIN_DOC_COUNT_FIELD);
        PARSER.declareObjectArray((agg, prefixes) -> {
            for (IpPrefix prefix: prefixes) {
                agg.addPrefix(prefix);
            }
        }, (p, c) -> IpPrefixAggregationBuilder.parseIpPrefix(p), RangeAggregator.RANGES_FIELD);
    }

    public IpPrefixAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        final int numPrefixes = in.readVInt();
        for (int i = 0; i < numPrefixes; ++i) {
            addPrefix(new IpPrefix(in));
        }
        keyed = in.readBoolean();
        isIpv6 = in.readBoolean();
    }

    private IpPrefixAggregationBuilder addPrefix(IpPrefix prefix) {
        ipPrefixes.add(prefix);
        return this;
    }

    private static IpPrefix parseIpPrefix(XContentParser parser) throws IOException {
        String key = null;
        boolean isIpv6 = false;
        String prefixLength = null;

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[ranges] must contain objects, but hit a " + parser.currentToken());
        }
        while(parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if(parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                continue;
            }
            if (KEY_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                key = parser.text();
            } else if (IS_IPV6.match(parser.currentName(), parser.getDeprecationHandler())) {
                isIpv6 = Boolean.parseBoolean(parser.text());
            } else if (PREFIX_LENGTH.match(parser.currentName(), parser.getDeprecationHandler())) {
                prefixLength = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected ip prefix parameter: [" + parser.currentName() + "]");
            }
        }
        if (prefixLength != null) {
            if (key == null) {
                key = prefixLength;
            }
            return new IpPrefix(key, isIpv6, Integer.parseInt(prefixLength));
        }

        throw new IllegalArgumentException("No [prefix_len] specified");
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        IpPrefixAggregatorFactory.registerAggregators(builder);
    }

    public static class IpPrefix implements ToXContentObject {
        private final String key;
        private final boolean isIpv6;
        private final Integer prefixLength;

        public IpPrefix(String key, boolean isIpv6, Integer prefixLength) {
            this.key = key;
            this.isIpv6 = isIpv6;
            this.prefixLength = prefixLength;
        }

        public IpPrefix(StreamInput in) throws IOException {
            this.key = in.readOptionalString();
            this.isIpv6 = in.readBoolean();
            this.prefixLength = in.readVInt();
        }

        public String getKey() {
            return key;
        }

        public boolean isIpv6() {
            return isIpv6;
        }

        public Integer getPrefixLength() {
            return prefixLength;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IpPrefix ipPrefix = (IpPrefix) o;
            return isIpv6 == ipPrefix.isIpv6 && Objects.equals(key, ipPrefix.key) && Objects.equals(prefixLength, ipPrefix.prefixLength);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, isIpv6, prefixLength);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            if (prefixLength != null) {
                builder.field(PREFIX_LENGTH.getPreferredName(), prefixLength);
            }
            builder.field(IS_IPV6.getPreferredName(), isIpv6);
            builder.endObject();
            return builder;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeBoolean(isIpv6);
            out.writeVInt(prefixLength);
        }
    }

    private boolean keyed = false;
    private long minDocCount = 0;
    private List<IpPrefix> ipPrefixes = new ArrayList<>();
    private boolean isIpv6 = false;

    public IpPrefixAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    public IpPrefixAggregationBuilder isIpv6(boolean isIpv6) {
        this.isIpv6 = isIpv6;
        return this;
    }

    public IpPrefixAggregationBuilder minDocCount(long minDOcCount) {
        this.minDocCount = minDOcCount;
        return this;
    }

    protected IpPrefixAggregationBuilder(String name) {
        super(name);
    }

    protected IpPrefixAggregationBuilder(
        IpPrefixAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.ipPrefixes = new ArrayList<>(clone.ipPrefixes);
        this.keyed = clone.keyed;
        this.isIpv6 = clone.isIpv6;
    }

    @Override
    protected AggregationBuilder shallowCopy(
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
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
        out.writeVInt(ipPrefixes.size());
        for (IpPrefix ipPrefix: ipPrefixes) {
            ipPrefix.writeTo(out);
        }
        out.writeBoolean(keyed);
        out.writeBoolean(isIpv6);
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

        List<IpPrefixAggregator.IpPrefix> prefixes = new ArrayList<>();
        if (this.ipPrefixes.size() == 0) {
            throw new IllegalArgumentException("No [prefix] specified for the [" + this.getName() + "] aggregation");
        }
        for (IpPrefixAggregationBuilder.IpPrefix ipPrefix : this.ipPrefixes) {
            byte[] subnet = extractSubnet(ipPrefix.prefixLength, ipPrefix.isIpv6);
            if (subnet == null) {
                throw new IllegalArgumentException(
                    "Unable to compute subnet for prefix length [" + ipPrefix.prefixLength + "] and ip version [" + (ipPrefix.isIpv6 ? "ipv6" : "ipv4") + "]"
                );
            }
            prefixes.add(
                new IpPrefixAggregator.IpPrefix(
                    ipPrefix.key,
                    ipPrefix.isIpv6,
                    subnet
                )
            );
        }

        return new IpPrefixAggregatorFactory(
            name,
            config,
            minDocCount,
            keyed,
            prefixes,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregationSupplier
        );
    }

    private static byte[] extractSubnet(int prefixLength, boolean isIpv6) {
        if(prefixLength < 0 || (!isIpv6 && prefixLength > 32) || (isIpv6 && prefixLength > 128)) {
            return null;
        }

        byte[] ipv4Address = { 0, 0, 0, 0 };
        byte[] ipv6Address = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        byte[] ipAddress = (isIpv6) ? ipv6Address : ipv4Address;
        int bytesCount = prefixLength / 8;
        int bitsCount = prefixLength % 8;
        int i = 0;
        for(; i < bytesCount; i++) {
            ipAddress[i] = (byte) 0xFF;
        }
        if(bitsCount > 0) {
            int rem = 0;
            for(int j = 0; j < bitsCount; j++) {
                rem |= 1 << (7 - j);
            }
            ipAddress[i] = (byte) rem;
        }

        try {
            return InetAddress.getByAddress(ipAddress).getAddress();
        } catch(UnknownHostException e) {
            return null;
        }
    }

    private static byte[] toIpv6(byte[] ipv4Address) {
        byte[] result = new byte[16];
        System.arraycopy(ipv4Address, 0, result, 16 - ipv4Address.length, ipv4Address.length);
        return result;
    }


    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ipPrefixes);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        builder.field(IS_IPV6.getPreferredName(), isIpv6);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        IpPrefixAggregationBuilder that = (IpPrefixAggregationBuilder) o;
        return keyed == that.keyed && isIpv6 == that.isIpv6 && Objects.equals(ipPrefixes, that.ipPrefixes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, ipPrefixes, isIpv6);
    }
}
