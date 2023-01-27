/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.prefix;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.test.MapMatcher;

import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class InternalIpPrefixTests extends InternalMultiBucketAggregationTestCase<InternalIpPrefix> {
    @Override
    protected InternalIpPrefix createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        return createTestInstance(name, metadata, aggregations, randomPrefixLength(), randomMinDocCount());
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation<?>> implementationClass() {
        // Deprecated high level rest client not supported
        return null;
    }

    private int randomPrefixLength() {
        return between(1, InetAddressPoint.BYTES * 8);
    }

    private long randomMinDocCount() {
        return randomBoolean() ? 1 : randomLongBetween(1, Long.MAX_VALUE / (maxNumberOfBuckets() + 1));
    }

    private InternalIpPrefix createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        int prefixLength,
        long minDocCount
    ) {
        boolean keyed = randomBoolean();
        boolean appendPrefixLength = randomBoolean();
        boolean canBeV4 = prefixLength <= 32;

        int bucketsCount = between(1, maxNumberOfBuckets());
        Set<BytesRef> keys = new TreeSet<>();
        while (keys.size() < bucketsCount) {
            boolean v4 = canBeV4 && randomBoolean();
            byte[] ip = InetAddressPoint.encode(randomIp(v4));
            byte[] mask = mask(v4 ? prefixLength + 96 : prefixLength);
            byte[] subnet = new byte[InetAddressPoint.BYTES];
            for (int i = 0; i < InetAddressPoint.BYTES; i++) {
                subnet[i] = (byte) (ip[i] & mask[i]);
            }
            keys.add(new BytesRef(ip));
        }
        List<InternalIpPrefix.Bucket> buckets = new ArrayList<>(keys.size());
        for (Iterator<BytesRef> itr = keys.iterator(); itr.hasNext();) {
            BytesRef key = itr.next();
            boolean v6 = InetAddressPoint.decode(key.bytes) instanceof Inet6Address;
            buckets.add(
                new InternalIpPrefix.Bucket(
                    DocValueFormat.IP,
                    key,
                    keyed,
                    v6,
                    prefixLength,
                    appendPrefixLength,
                    randomLongBetween(0, Long.MAX_VALUE),
                    aggregations
                )
            );
        }

        return new InternalIpPrefix(name, DocValueFormat.IP, keyed, minDocCount, buckets, metadata);
    }

    private byte[] mask(int prefixLength) {
        byte[] mask = new byte[InetAddressPoint.BYTES];
        int m = 0;
        int b = 0x80;
        for (int i = 0; i < prefixLength; i++) {
            mask[m] |= b;
            b = b >> 1;
            if (b == 0) {
                m++;
                b = 0x80;
            }
        }
        return mask;
    }

    @Override
    protected BuilderAndToReduce<InternalIpPrefix> randomResultsToReduce(String name, int size) {
        Map<String, Object> metadata = createTestMetadata();
        InternalAggregations aggregations = createSubAggregations();
        int prefixLength = randomPrefixLength();
        long minDocCount = randomMinDocCount();
        List<InternalIpPrefix> inputs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            InternalIpPrefix t = createTestInstance(name, metadata, aggregations, prefixLength, minDocCount);
            inputs.add(t);
        }
        return new BuilderAndToReduce<>(mockBuilder(inputs), inputs);
    }

    @Override
    protected void assertReduced(InternalIpPrefix reduced, List<InternalIpPrefix> inputs) {
        InternalIpPrefix leader = inputs.get(0);
        assertThat(reduced.keyed, equalTo(leader.keyed));
        assertThat(reduced.format, equalTo(leader.format));
        assertThat(reduced.minDocCount, equalTo(leader.minDocCount));
        Map<BytesRef, Long> expectedCounts = new HashMap<>();
        for (InternalIpPrefix i : inputs) {
            for (InternalIpPrefix.Bucket b : i.getBuckets()) {
                assertThat(b.getFormat(), equalTo(DocValueFormat.IP));
                long acc = expectedCounts.getOrDefault(b.getKey(), 0L);
                acc += b.getDocCount();
                expectedCounts.put(b.getKey(), acc);
            }
        }
        MapMatcher countsMatches = matchesMap();
        for (Map.Entry<BytesRef, Long> e : expectedCounts.entrySet()) {
            if (e.getValue() >= leader.minDocCount) {
                countsMatches = countsMatches.entry(DocValueFormat.IP.format(e.getKey()), e.getValue());
            }
        }
        assertMap(
            new TreeMap<>(reduced.getBuckets().stream().collect(toMap(b -> b.getKeyAsString(), b -> b.getDocCount()))),
            countsMatches
        );
    }

    public void testPartialReduceNoMinDocCount() {
        InternalIpPrefix.Bucket b1 = new InternalIpPrefix.Bucket(
            DocValueFormat.IP,
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.0.1"))),
            false,
            false,
            1,
            false,
            1,
            InternalAggregations.EMPTY
        );
        InternalIpPrefix.Bucket b2 = new InternalIpPrefix.Bucket(
            DocValueFormat.IP,
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("200.0.0.1"))),
            false,
            false,
            1,
            false,
            2,
            InternalAggregations.EMPTY
        );
        InternalIpPrefix t = new InternalIpPrefix("test", DocValueFormat.IP, false, 100, List.of(b1, b2), null);
        InternalIpPrefix reduced = (InternalIpPrefix) t.reduce(List.of(t), emptyReduceContextBuilder().forPartialReduction());
        assertThat(reduced.getBuckets().get(0).getDocCount(), equalTo(1L));
        assertThat(reduced.getBuckets().get(1).getDocCount(), equalTo(2L));
    }

    @Override
    protected InternalIpPrefix mutateInstance(InternalIpPrefix instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
