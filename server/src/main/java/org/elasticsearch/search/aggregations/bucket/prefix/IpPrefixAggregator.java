/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
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
        final SortedBinaryDocValues values = config.getValuesSource().bytesValues(aggCtx.getLeafReaderContext());
        final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
        return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
    }

    private LeafBucketCollector getLeafCollector(SortedBinaryDocValues values, LeafBucketCollector sub) {

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    BytesRef previousSubnet = null;
                    for (int i = 0; i < values.docValueCount(); ++i) {
                        final BytesRef subnet = new BytesRef(new byte[ipPrefix.netmask.length]);
                        maskIpAddress(values.nextValue(), ipPrefix.netmask, subnet);
                        if (previousSubnet != null && subnet.bytesEquals(previousSubnet)) {
                            continue;
                        }
                        addBucketOrd(bucketOrds.add(owningBucketOrd, subnet), doc, sub);
                        previousSubnet = subnet;
                    }
                }
            }
        };
    }

    private LeafBucketCollector getLeafCollector(BinaryDocValues values, LeafBucketCollector sub) {
        final BytesRef subnet = new BytesRef(new byte[ipPrefix.netmask.length]);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    maskIpAddress(values.binaryValue(), ipPrefix.netmask, subnet);
                    addBucketOrd(bucketOrds.add(owningBucketOrd, subnet), doc, sub);
                }
            }
        };
    }

    private void addBucketOrd(long bucketOrd, int doc, LeafBucketCollector sub) throws IOException {
        if (bucketOrd < 0) {
            bucketOrd = -1 - bucketOrd;
            collectExistingBucket(sub, doc, bucketOrd);
        } else {
            collectBucket(sub, doc, bucketOrd);
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

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        long totalOrdsToCollect = 0;
        try (IntArray bucketsInOrd = bigArrays().newIntArray(owningBucketOrds.size())) {
            for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                final long bucketCount = bucketOrds.bucketsInOrd(owningBucketOrds.get(ordIdx));
                bucketsInOrd.set(ordIdx, (int) bucketCount);
                totalOrdsToCollect += bucketCount;
            }

            try (LongArray bucketOrdsToCollect = bigArrays().newLongArray(totalOrdsToCollect)) {
                int[] b = new int[] { 0 };
                for (long i = 0; i < owningBucketOrds.size(); i++) {
                    BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds.get(i));
                    while (ordsEnum.next()) {
                        bucketOrdsToCollect.set(b[0]++, ordsEnum.ord());
                    }
                }

                var subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);
                b[0] = 0;
                return buildAggregations(Math.toIntExact(owningBucketOrds.size()), ordIdx -> {
                    List<InternalIpPrefix.Bucket> buckets = new ArrayList<>(bucketsInOrd.get(ordIdx));
                    BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds.get(ordIdx));
                    while (ordsEnum.next()) {
                        long ordinal = ordsEnum.ord();
                        if (bucketOrdsToCollect.get(b[0]) != ordinal) {
                            throw AggregationErrors.iterationOrderChangedWithoutMutating(
                                bucketOrds.toString(),
                                ordinal,
                                bucketOrdsToCollect.get(b[0])
                            );
                        }
                        BytesRef ipAddress = new BytesRef();
                        ordsEnum.readValue(ipAddress);
                        long docCount = bucketDocCount(ordinal);
                        checkRealMemoryCBForInternalBucket();
                        buckets.add(
                            new InternalIpPrefix.Bucket(
                                BytesRef.deepCopyOf(ipAddress),
                                ipPrefix.isIpv6,
                                ipPrefix.prefixLength,
                                ipPrefix.appendPrefixLength,
                                docCount,
                                subAggregationResults.apply(b[0]++)
                            )
                        );

                        // NOTE: the aggregator is expected to return sorted results
                        CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());
                    }
                    return new InternalIpPrefix(name, config.format(), keyed, minDocCount, buckets, metadata());
                });
            }
        }
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
