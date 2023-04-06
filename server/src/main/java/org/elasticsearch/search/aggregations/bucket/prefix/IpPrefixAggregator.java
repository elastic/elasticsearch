/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An IP prefix aggregator for IPv6 or IPv4 subnets.
 */
public final class IpPrefixAggregator extends BucketsAggregator {

    public static class IpPrefix {
        final boolean isIpv6;
        final int prefixLength;
        final boolean appendPrefixLength;
        final BytesRef netmask;

        public IpPrefix(boolean isIpv6, int prefixLength, boolean appendPrefixLength, BytesRef netmask) {
            this.isIpv6 = isIpv6;
            this.prefixLength = prefixLength;
            this.appendPrefixLength = appendPrefixLength;
            this.netmask = netmask;
        }

        public boolean isIpv6() {
            return isIpv6;
        }

        public int getPrefixLength() {
            return prefixLength;
        }

        public boolean appendPrefixLength() {
            return appendPrefixLength;
        }

        public BytesRef getNetmask() {
            return netmask;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IpPrefix ipPrefix = (IpPrefix) o;
            return isIpv6 == ipPrefix.isIpv6
                && prefixLength == ipPrefix.prefixLength
                && appendPrefixLength == ipPrefix.appendPrefixLength
                && Objects.equals(netmask, ipPrefix.netmask);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isIpv6, prefixLength, appendPrefixLength, netmask);
        }
    }

    final ValuesSourceConfig config;
    final long minDocCount;
    final boolean keyed;
    final BytesKeyedBucketOrds bucketOrds;
    final IpPrefix ipPrefix;

    public IpPrefixAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSourceConfig config,
        boolean keyed,
        long minDocCount,
        IpPrefix ipPrefix,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.config = config;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), cardinality);
        this.ipPrefix = ipPrefix;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        return new IpPrefixLeafCollector(sub, config.getValuesSource().bytesValues(aggCtx.getLeafReaderContext()), ipPrefix);
    }

    private class IpPrefixLeafCollector extends LeafBucketCollectorBase {
        private final IpPrefix ipPrefix;
        private final LeafBucketCollector sub;
        private final SortedBinaryDocValues values;

        IpPrefixLeafCollector(final LeafBucketCollector sub, final SortedBinaryDocValues values, final IpPrefix ipPrefix) {
            super(sub, values);
            this.sub = sub;
            this.values = values;
            this.ipPrefix = ipPrefix;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            BytesRef previousSubnet = null;
            BytesRef subnet = new BytesRef(new byte[ipPrefix.netmask.length]);
            BytesRef ipAddress;
            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();

                for (int i = 0; i < valuesCount; ++i) {
                    ipAddress = values.nextValue();
                    maskIpAddress(ipAddress, ipPrefix.netmask, subnet);
                    if (previousSubnet != null && subnet.bytesEquals(previousSubnet)) {
                        continue;
                    }
                    long bucketOrd = bucketOrds.add(owningBucketOrd, subnet);
                    if (bucketOrd < 0) {
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        collectBucket(sub, doc, bucketOrd);
                    }
                    previousSubnet = subnet;
                }
            }
        }

        private static void maskIpAddress(final BytesRef ipAddress, final BytesRef subnetMask, final BytesRef subnet) {
            assert ipAddress.length == 16 : "Invalid length for ip address [" + ipAddress.length + "] expected 16 bytes";
            // NOTE: IPv4 addresses are encoded as 16-bytes. As a result, we use an
            // offset (12) to apply the subnet to the last 4 bytes (byes 12, 13, 14, 15)
            // if the subnet mask is just a 4-bytes subnet mask.
            int offset = subnetMask.length == 4 ? 12 : 0;
            for (int i = 0; i < subnetMask.length; ++i) {
                subnet.bytes[i] = (byte) (ipAddress.bytes[i + offset] & subnetMask.bytes[i]);
            }
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        long totalOrdsToCollect = 0;
        final int[] bucketsInOrd = new int[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            final long bucketCount = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
            bucketsInOrd[ordIdx] = (int) bucketCount;
            totalOrdsToCollect += bucketCount;
        }

        long[] bucketOrdsToCollect = new long[(int) totalOrdsToCollect];
        int b = 0;
        for (long owningBucketOrd : owningBucketOrds) {
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
            while (ordsEnum.next()) {
                bucketOrdsToCollect[b++] = ordsEnum.ord();
            }
        }

        InternalAggregations[] subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        b = 0;
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            List<InternalIpPrefix.Bucket> buckets = new ArrayList<>(bucketsInOrd[ordIdx]);
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while (ordsEnum.next()) {
                long ordinal = ordsEnum.ord();
                if (bucketOrdsToCollect[b] != ordinal) {
                    throw new AggregationExecutionException(
                        "Iteration order of ["
                            + bucketOrds
                            + "] changed without mutating. ["
                            + ordinal
                            + "] should have been ["
                            + bucketOrdsToCollect[b]
                            + "]"
                    );
                }
                BytesRef ipAddress = new BytesRef();
                ordsEnum.readValue(ipAddress);
                long docCount = bucketDocCount(ordinal);
                buckets.add(
                    new InternalIpPrefix.Bucket(
                        config.format(),
                        BytesRef.deepCopyOf(ipAddress),
                        keyed,
                        ipPrefix.isIpv6,
                        ipPrefix.prefixLength,
                        ipPrefix.appendPrefixLength,
                        docCount,
                        subAggregationResults[b++]
                    )
                );

                // NOTE: the aggregator is expected to return sorted results
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());
            }
            results[ordIdx] = new InternalIpPrefix(name, config.format(), keyed, minDocCount, buckets, metadata());
        }
        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalIpPrefix(name, config.format(), keyed, minDocCount, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    public static class Unmapped extends NonCollectingAggregator {

        private final ValuesSourceConfig config;
        private final boolean keyed;
        private final long minDocCount;

        protected Unmapped(
            String name,
            AggregatorFactories factories,
            ValuesSourceConfig config,
            boolean keyed,
            long minDocCount,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, context, parent, factories, metadata);
            this.config = config;
            this.keyed = keyed;
            this.minDocCount = minDocCount;
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new InternalIpPrefix(name, config.format(), keyed, minDocCount, Collections.emptyList(), metadata());
        }
    }
}
