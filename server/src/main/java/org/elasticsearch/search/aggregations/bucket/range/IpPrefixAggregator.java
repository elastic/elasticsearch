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
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class IpPrefixAggregator extends BucketsAggregator {

    public static class IpPrefix {
        final String key;
        final boolean isIpv6;
        final byte[] mask;

        public IpPrefix(
            String key,
            boolean isIpv6,
            byte[] mask
        ) {
            this.key = key;
            this.isIpv6 = isIpv6;
            this.mask = mask;
        }

        public String getKey() {
            return key;
        }

        public boolean isIpv6() {
            return isIpv6;
        }

        public byte[] getMask() {
            return mask;
        }
    }

    private final Comparator<IpPrefix> IP_PREFIX_COMPARATOR = ((Comparator<IpPrefix>) (ipA, ipB) -> Arrays.compare(ipA.mask, ipB.mask))
        .thenComparing((ipA, ipB) -> Boolean.compare(ipA.isIpv6, ipB.isIpv6))
        .thenComparing(Comparator.nullsLast(Comparator.comparing(IpPrefix::getKey)));

    private final boolean keyed;
    private final IpPrefix[] ipPrefixes;

    final ValuesSource.Bytes valuesSource;
    final DocValueFormat format;
    private final long minDocCount;
    private final BytesKeyedBucketOrds bucketOrds;

    public IpPrefixAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource valuesSource,
        DocValueFormat format,
        long minDocCount,
        List<IpPrefix> ipPrefixes,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        this.valuesSource = (ValuesSource.Bytes) valuesSource;
        this.format = format;
        this.minDocCount = minDocCount;
        this.keyed = keyed;
        this.bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), cardinality);
        this.ipPrefixes = ipPrefixes.toArray(new IpPrefix[0]);
        Arrays.sort(this.ipPrefixes, IP_PREFIX_COMPARATOR);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(
        LeafReaderContext ctx,
        LeafBucketCollector sub
    ) throws IOException {
        return valuesSource == null ?
            LeafBucketCollector.NO_OP_COLLECTOR : new IpPrefixLeafCollector(sub, valuesSource.bytesValues(ctx), ipPrefixes);
    }

    private class IpPrefixLeafCollector extends LeafBucketCollectorBase {
        private final IpPrefix[] ipPrefixes;
        private final LeafBucketCollector sub;
        private final SortedBinaryDocValues values;

        public IpPrefixLeafCollector(
            LeafBucketCollector sub,
            SortedBinaryDocValues values,
            IpPrefix[] ipPrefixes
        ) {
            super(sub, values);
            this.sub = sub;
            this.values = values;
            this.ipPrefixes = ipPrefixes;
            for (int i = 1; i < ipPrefixes.length; ++i) {
                if (IP_PREFIX_COMPARATOR.compare(ipPrefixes[i - 1], ipPrefixes[i]) > 0) {
                    throw new IllegalArgumentException("Ip prefixes must be sorted");
                }
            }
        }

        @Override
        public void collect(
            int doc,
            long owningBucketOrd
        ) throws IOException {
            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();

                byte[] previousSubnet = null;
                for (int i = 0; i < valuesCount; ++i) {
                    BytesRef ipAddress = values.nextValue();
                    for (IpPrefix ipPrefix : ipPrefixes) {
                        byte[] subnet = maskIpAddress(ipAddress.bytes, ipPrefix.mask);
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
        }

        private byte[] maskIpAddress(byte[] ipAddress, byte[] subnetMask) {
            //NOTE: ip addresses are always encoded to 16 bytes by IpFieldMapper
            if (ipAddress.length != 16) {
                throw new IllegalArgumentException("Invalid length for ip address [" + ipAddress.length + "]");
            }
            if (subnetMask.length == 4) {
                return mask(Arrays.copyOfRange(ipAddress, 12, 16), subnetMask);
            }
            if (subnetMask.length == 16) {
                return mask(ipAddress, subnetMask);
            }

            throw new IllegalArgumentException("Invalid length for subnet mask [" + subnetMask.length + "]");
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
    public InternalAggregation[] buildAggregations(
        long[] owningBucketOrds
    ) throws IOException {
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
                        format,
                        keyed,
                        null, //ipAddress.toString(), //TODO: #57964 (use the key from the request)
                        BytesRef.deepCopyOf(ipAddress),
                        docCount,
                        subAggregationResults[b++]
                    )
                );

                // NOTE: the aggregator is expected to return sorted results
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());
            }
            results[ordIdx] = new InternalIpPrefix(name, format, minDocCount, buckets, keyed, metadata());
        }
        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalIpPrefix(
            name,
            format,
            minDocCount,
            Collections.emptyList(),
            keyed,
            metadata()
        );
    }
}
