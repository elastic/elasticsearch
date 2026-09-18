/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.BinaryDocValuesField;
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

import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL;
import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.SEPARATE_COUNT;

public class ScanningBinaryDocValuesRangeQueryTests extends ESTestCase {

    public void testRangeUsesBytesRefOrdering() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(docWithValue(fieldName, new String(Character.toChars(0x1F600))));
                writer.addDocument(docWithValue(fieldName, "\uF000"));
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("\uE000"),
                        null,
                        true,
                        true,
                        SEPARATE_COUNT
                    );
                    assertEquals(2, searcher.count(query));
                }
            }
        }
    }

    public void testRangeSupportsOpenAndExclusiveBounds() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(docWithValue(fieldName, "alpha"));
                writer.addDocument(docWithValue(fieldName, "beta"));
                writer.addDocument(docWithValue(fieldName, "gamma"));
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query closed = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("alpha"),
                        new BytesRef("gamma"),
                        true,
                        true,
                        SEPARATE_COUNT
                    );
                    assertEquals(3, searcher.count(closed));

                    Query lowerOpen = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        null,
                        new BytesRef("gamma"),
                        true,
                        false,
                        SEPARATE_COUNT
                    );
                    assertEquals(2, searcher.count(lowerOpen));

                    Query upperOpen = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("alpha"),
                        null,
                        false,
                        true,
                        SEPARATE_COUNT
                    );
                    assertEquals(2, searcher.count(upperOpen));
                }
            }
        }
    }

    public void testArrayOrderInlineNull() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = ArrayOrderInlineNullTestUtils.newWriter(dir)) {
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "alpha", null, "beta"); // multi-value with an inline null slot
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, (String) null);          // all-null, immediately before a match
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "beta");                  // single value stored raw
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName);                          // empty array
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "gamma", "delta");        // multi-value
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // [alpha, beta] matches "alpha"/"beta" in the multi-value doc and "beta" in the single-value doc; the all-null doc
                    // preceding the latter must not be matched.
                    var alphaToBeta = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("alpha"),
                        new BytesRef("beta"),
                        ARRAY_ORDER_INLINE_NULL
                    );
                    assertEquals(2, searcher.count(alphaToBeta));
                    // [delta, gamma] matches only the last doc ("delta"/"gamma").
                    var deltaToGamma = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("delta"),
                        new BytesRef("gamma"),
                        ARRAY_ORDER_INLINE_NULL
                    );
                    assertEquals(1, searcher.count(deltaToGamma));
                }
            }
        }
    }

    private static BytesRef encodeIp(String ip) {
        return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
    }

    private static Document docWithValue(String fieldName, String value) {
        Document document = new Document();
        document.add(new BinaryDocValuesField(fieldName, new BytesRef(value)));
        return document;
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

    private static Document docWithValues(String fieldName, String... values) {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
            fieldName,
            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
        );
        for (String value : values) {
            field.add(new BytesRef(value));
        }
        document.add(field);
        document.add(new NumericDocValuesField(fieldName + ".counts", field.count()));
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
                    Query query = new ScanningBinaryDocValuesRangeQuery(fieldName, lower, upper, SEPARATE_COUNT);
                    assertEquals(2, searcher.count(query));
                }
            }
        }
    }

    public void testOpenRangeMatchesMultiValued() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(docWithValues(fieldName, "gamma"));
                writer.addDocument(docWithValues(fieldName, "alpha"));
                writer.addDocument(docWithValues(fieldName, "alpha", "beta"));
                writer.addDocument(docWithValues(fieldName, "delta", "epsilon"));
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query closed = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("beta"),
                        new BytesRef("delta"),
                        true,
                        true,
                        SEPARATE_COUNT
                    );
                    assertEquals(2, searcher.count(closed));

                    Query lowerOpen = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        null,
                        new BytesRef("beta"),
                        true,
                        true,
                        SEPARATE_COUNT
                    );
                    assertEquals(2, searcher.count(lowerOpen));

                    Query upperOpen = new ScanningBinaryDocValuesRangeQuery(
                        fieldName,
                        new BytesRef("delta"),
                        null,
                        true,
                        true,
                        SEPARATE_COUNT
                    );
                    assertEquals(2, searcher.count(upperOpen));
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
                    Query query = new ScanningBinaryDocValuesRangeQuery(fieldName, lower, upper, SEPARATE_COUNT);
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
                    Query query = new ScanningBinaryDocValuesRangeQuery(fieldName, lower, upper, SEPARATE_COUNT);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testRewriteToTermQueryWhenBoundsEqual() throws Exception {
        BytesRef term = new BytesRef("alpha");
        ScanningBinaryDocValuesRangeQuery range = new ScanningBinaryDocValuesRangeQuery("field", term, term, SEPARATE_COUNT);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(new ScanningBinaryDocValuesTermQuery("field", term, SEPARATE_COUNT), range.rewrite(searcher));
                }
            }
        }
    }

    public void testRewriteKeepsExclusiveEqualBoundsRange() throws Exception {
        BytesRef term = new BytesRef("alpha");
        ScanningBinaryDocValuesRangeQuery range = new ScanningBinaryDocValuesRangeQuery("field", term, term, false, true, SEPARATE_COUNT);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertSame(range, range.rewrite(searcher));
                }
            }
        }
    }

    public void testRewriteKeepsTrueRange() throws Exception {
        BytesRef lower = encodeIp("192.168.1.0");
        BytesRef upper = encodeIp("192.168.1.255");
        ScanningBinaryDocValuesRangeQuery range = new ScanningBinaryDocValuesRangeQuery("field", lower, upper, SEPARATE_COUNT);
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
