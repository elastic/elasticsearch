/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

    final ValuesSource.Bytes valuesSource;
    final DocValueFormat format;
    final long minDocCount;
    final boolean keyed;
    final BytesKeyedBucketOrds bucketOrds;
    final IpPrefix ipPrefix;

    public IpPrefixAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource valuesSource,
        DocValueFormat format,
        boolean keyed,
        long minDocCount,
        IpPrefix ipPrefix,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.valuesSource = (ValuesSource.Bytes) valuesSource;
        this.format = format;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), cardinality);
        this.ipPrefix = ipPrefix;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return valuesSource == null
            ? LeafBucketCollector.NO_OP_COLLECTOR
            : new IpPrefixLeafCollector(sub, valuesSource.bytesValues(ctx), ipPrefix);
    }

    private class IpPrefixLeafCollector extends LeafBucketCollectorBase {
        private final IpPrefix ipPrefix;
        private final LeafBucketCollector sub;
        private final SortedBinaryDocValues values;

        IpPrefixLeafCollector(LeafBucketCollector sub, SortedBinaryDocValues values, IpPrefix ipPrefix) {
            super(sub, values);
            this.sub = sub;
            this.values = values;
            this.ipPrefix = ipPrefix;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();

                byte[] previousSubnet = null;
                for (int i = 0; i < valuesCount; ++i) {
                    BytesRef value = values.nextValue();
                    byte[] ipAddress = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
                    byte[] netmask = Arrays.copyOfRange(
                        ipPrefix.netmask.bytes,
                        ipPrefix.netmask.offset,
                        ipPrefix.netmask.offset + ipPrefix.netmask.length
                    );
                    byte[] subnet = maskIpAddress(ipAddress, netmask);
                    if (Arrays.equals(subnet, previousSubnet)) {
                        continue;
                    }
                    long bucketOrd = bucketOrds.add(owningBucketOrd, new BytesRef(subnet));
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

        private byte[] maskIpAddress(byte[] ipAddress, byte[] netmask) {
            // NOTE: ip addresses are always encoded to 16 bytes by IpFieldMapper
            if (ipAddress.length != 16) {
                throw new IllegalArgumentException("Invalid length for ip address [" + ipAddress.length + "]");
            }
            if (netmask.length == 4) {
                return mask(Arrays.copyOfRange(ipAddress, 12, 16), netmask);
            }
            if (netmask.length == 16) {
                return mask(ipAddress, netmask);
            }

            throw new IllegalArgumentException("Invalid length for netmask [" + netmask.length + "]");
        }

        private byte[] mask(byte[] ipAddress, byte[] subnetMask) {
            byte[] subnet = new byte[ipAddress.length];
            for (int i = 0; i < ipAddress.length; ++i) {
                subnet[i] = (byte) (ipAddress[i] & subnetMask[i]);
            }

            return subnet;
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
        if (totalOrdsToCollect > Integer.MAX_VALUE) {
            throw new AggregationExecutionException(
                "Can't collect more than [" + Integer.MAX_VALUE + "] buckets but attempted [" + totalOrdsToCollect + "]"
            );
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
                        format,
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
            results[ordIdx] = new InternalIpPrefix(name, format, keyed, minDocCount, buckets, metadata());
        }
        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalIpPrefix(name, format, keyed, minDocCount, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
