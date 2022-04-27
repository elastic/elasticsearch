/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Comparator;

public class IpRangeAggregatorTests extends AggregatorTestCase {

    private static boolean isInRange(BytesRef value, BytesRef from, BytesRef to) {
        if ((to == null || to.compareTo(value) > 0) && (from == null || from.compareTo(value) <= 0)) {
            return true;
        }
        return false;
    }

    private static final Comparator<Tuple<BytesRef, BytesRef>> RANGE_COMPARATOR = (a, b) -> {
        int cmp = compare(a.v1(), b.v1(), 1);
        if (cmp == 0) {
            cmp = compare(a.v2(), b.v2(), -1);
        }
        return cmp;
    };

    private static int compare(BytesRef a, BytesRef b, int m) {
        return a == null ? b == null ? 0 : -m : b == null ? m : a.compareTo(b);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRanges() throws Exception {
        boolean v4 = randomBoolean();
        IpRangeAggregationBuilder builder = new IpRangeAggregationBuilder("test_agg").field("field");
        int numRanges = randomIntBetween(1, 10);
        Tuple<BytesRef, BytesRef>[] requestedRanges = new Tuple[numRanges];
        for (int i = 0; i < numRanges; i++) {
            Tuple<InetAddress, BytesRef>[] arr = new Tuple[2];
            for (int j = 0; j < 2; j++) {
                InetAddress addr = randomIp(v4);
                arr[j] = new Tuple(addr, new BytesRef(InetAddressPoint.encode(addr)));
            }
            Arrays.sort(arr, (t1, t2) -> t1.v2().compareTo(t2.v2()));
            if (rarely()) {
                if (randomBoolean()) {
                    builder.addRange(NetworkAddress.format(arr[0].v1()), null);
                    requestedRanges[i] = new Tuple(arr[0].v2(), null);
                } else {
                    builder.addRange(null, NetworkAddress.format(arr[1].v1()));
                    requestedRanges[i] = new Tuple(null, arr[1].v2());
                }
            } else {
                builder.addRange(NetworkAddress.format(arr[0].v1()), NetworkAddress.format(arr[1].v1()));
                requestedRanges[i] = new Tuple(arr[0].v2(), arr[1].v2());
            }
        }
        Arrays.sort(requestedRanges, RANGE_COMPARATOR);
        int[] expectedCounts = new int[numRanges];
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            int numDocs = randomIntBetween(10, 100);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                BytesRef[] values = new BytesRef[numValues];
                for (int j = 0; j < numValues; j++) {
                    values[j] = new BytesRef(InetAddressPoint.encode(randomIp(v4)));
                    doc.add(new SortedSetDocValuesField("field", values[j]));
                }
                Arrays.sort(values);
                for (int j = 0; j < numRanges; j++) {
                    for (int k = 0; k < numValues; k++) {
                        if (isInRange(values[k], requestedRanges[j].v1(), requestedRanges[j].v2())) {
                            expectedCounts[j]++;
                            break;
                        }
                    }
                }
                w.addDocument(doc);
            }
            MappedFieldType fieldType = new IpFieldMapper.IpFieldType("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalBinaryRange range = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, fieldType);
                assertEquals(numRanges, range.getBuckets().size());
                for (int i = 0; i < range.getBuckets().size(); i++) {
                    Tuple<BytesRef, BytesRef> expected = requestedRanges[i];
                    Range.Bucket bucket = range.getBuckets().get(i);
                    if (expected.v1() == null) {
                        assertNull(bucket.getFrom());
                    } else {
                        assertEquals(DocValueFormat.IP.format(expected.v1()), bucket.getFrom());
                    }
                    if (expected.v2() == null) {
                        assertNull(bucket.getTo());
                    } else {
                        assertEquals(DocValueFormat.IP.format(expected.v2()), bucket.getTo());
                    }
                    assertEquals(expectedCounts[i], bucket.getDocCount());
                }
            }
        }
    }

    public void testMissingUnmapped() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                Document doc = new Document();
                w.addDocument(doc);
            }

            IpRangeAggregationBuilder builder = new IpRangeAggregationBuilder("test_agg").field("field")
                .addRange(new IpRangeAggregationBuilder.Range("foo", "192.168.100.0", "192.168.100.255"))
                .missing("192.168.100.42"); // Apparently we expect a string here
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalBinaryRange range = searchAndReduce(searcher, new MatchAllDocsQuery(), builder);
                assertEquals(1, range.getBuckets().size());
            }
        }
    }

    public void testMissingUnmappedBadType() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 7; i++) {
                Document doc = new Document();
                w.addDocument(doc);
            }

            IpRangeAggregationBuilder builder = new IpRangeAggregationBuilder("test_agg").field("field")
                .addRange(new IpRangeAggregationBuilder.Range("foo", "192.168.100.0", "192.168.100.255"))
                .missing(1234);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                expectThrows(IllegalArgumentException.class, () -> searchAndReduce(searcher, new MatchAllDocsQuery(), builder));
            }
        }
    }
}
