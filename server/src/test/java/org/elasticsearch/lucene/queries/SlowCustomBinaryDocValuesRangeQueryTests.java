/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SlowCustomBinaryDocValuesRangeQueryTests extends ESTestCase {

    private static BytesRef encodeIp(String ip) {
        return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
    }

    private static Document docWithIps(String... ips) {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE);
        for (String ip : ips) {
            field.add(encodeIp(ip));
        }
        document.add(field);
        document.add(new NumericDocValuesField("field.counts", field.count()));
        return document;
    }

    public void testRangeMatchesSingleAndMultiValued() throws Exception {
        String fieldName = "field";
        BytesRef lower = encodeIp("192.168.1.0");
        BytesRef upper = encodeIp("192.168.1.255");
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(docWithIps("192.168.1.50"));
                writer.addDocument(docWithIps("10.0.0.1"));
                writer.addDocument(docWithIps("10.0.0.2", "192.168.1.7"));
                writer.addDocument(docWithIps("10.0.0.3", "10.0.0.4"));
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesRangeQuery(fieldName, lower, upper);
                    assertEquals(2, searcher.count(query));
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";
        BytesRef lower = encodeIp("192.168.1.0");
        BytesRef upper = encodeIp("192.168.1.255");
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesRangeQuery(fieldName, lower, upper);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(docWithIps("192.168.1.1"));
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesRangeQuery(fieldName, lower, upper);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testRewriteToTermQueryWhenBoundsEqual() throws Exception {
        BytesRef term = encodeIp("192.168.1.1");
        SlowCustomBinaryDocValuesRangeQuery range = new SlowCustomBinaryDocValuesRangeQuery("field", term, term);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(new SlowCustomBinaryDocValuesTermQuery("field", term), range.rewrite(searcher));
                }
            }
        }
    }

    public void testRewriteKeepsTrueRange() throws Exception {
        BytesRef lower = encodeIp("192.168.1.0");
        BytesRef upper = encodeIp("192.168.1.255");
        SlowCustomBinaryDocValuesRangeQuery range = new SlowCustomBinaryDocValuesRangeQuery("field", lower, upper);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertSame(range, range.rewrite(searcher));
                }
            }
        }
    }
}
