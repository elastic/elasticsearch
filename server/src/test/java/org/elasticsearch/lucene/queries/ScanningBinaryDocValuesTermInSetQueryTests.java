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
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScanningBinaryDocValuesTermInSetQueryTests extends ESTestCase {

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
                    // {beta, zeta}: beta is in the multi-value and single-value docs (the all-null doc preceding the latter must not be
                    // matched), zeta is absent.
                    var betaOrZeta = List.of(new BytesRef("beta"), new BytesRef("zeta"));
                    var format = BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL;
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, betaOrZeta, format)));
                    // {alpha, delta}: alpha is in the first doc, delta in the last.
                    var alphaOrDelta = List.of(new BytesRef("alpha"), new BytesRef("delta"));
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermInSetQuery(fieldName, alphaOrDelta, format)));
                }
            }
        }
    }

    public void testToStringWithTermsThatAreNotUtf8() {
        BytesRef notUtf8 = new BytesRef(new byte[] { (byte) 0xff, 0x00, (byte) 0xfe, 0x01 });
        Query query = new ScanningBinaryDocValuesTermInSetQuery(
            "field",
            List.of(notUtf8, new BytesRef("ascii")),
            BinaryDocValuesFormat.SEPARATE_COUNT
        );
        assertEquals("ScanningBinaryDocValuesTermInSetQuery(fieldName=field,terms=[ascii,[ff 0 fe 1]])", query.toString("field"));
    }

    public void testManyTerms() throws Exception {
        int numTerms = randomIntBetween(2000, 5000);
        List<BytesRef> members = new ArrayList<>();
        List<BytesRef> others = new ArrayList<>();
        for (int i = 0; i < numTerms; i++) {
            byte[] member = new byte[randomIntBetween(0, 20)];
            random().nextBytes(member);
            members.add(new BytesRef(member));
            others.add(new BytesRef(Arrays.copyOf(member, member.length + 1)));
        }
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            for (BytesRef term : members) {
                writer.addDocument(singleValueDoc(term));
            }
            for (BytesRef term : others) {
                writer.addDocument(singleValueDoc(term));
            }
            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                var format = BinaryDocValuesFormat.SEPARATE_COUNT;
                Set<BytesRef> memberSet = new HashSet<>(members);
                Set<BytesRef> otherSet = new HashSet<>(others);
                long expectedMembers = members.stream().filter(memberSet::contains).count() + others.stream()
                    .filter(memberSet::contains)
                    .count();
                long expectedOthers = members.stream().filter(otherSet::contains).count() + others.stream()
                    .filter(otherSet::contains)
                    .count();
                assertEquals(expectedMembers, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", members, format)));
                assertEquals(expectedOthers, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", others, format)));
            }
        }
    }

    public void testValuesLongerThanEveryTermDoNotMatch() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(singleValueDoc(new BytesRef("ab")));
            writer.addDocument(singleValueDoc(new BytesRef("abc")));
            writer.addDocument(singleValueDoc(new BytesRef(new byte[10_000])));
            writer.addDocument(singleValueDoc(new BytesRef("")));
            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                var format = BinaryDocValuesFormat.SEPARATE_COUNT;
                var terms = List.of(new BytesRef("ab"), new BytesRef(""));
                assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", terms, format)));
            }
        }
    }

    private static Document singleValueDoc(BytesRef term) {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE);
        field.add(term);
        document.add(field);
        document.add(new NumericDocValuesField("field.counts", 1));
        return document;
    }

    public void testRawBytesTerms() throws Exception {
        List<BytesRef> values = List.of(
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("10.0.0.1"))),
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("10.0.0.2"))),
            new BytesRef(InetAddressPoint.encode(InetAddresses.forString("2001:db8::2:1")))
        );
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            for (int doc = 0; doc < 12; doc++) {
                Document document = new Document();
                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    "field",
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                field.add(values.get(doc % values.size()));
                var countField = new NumericDocValuesField("field.counts", 1);
                if (doc % 4 == 0) {
                    field.add(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::ffff:1"))));
                    countField.setLongValue(field.count());
                }
                document.add(field);
                document.add(countField);
                writer.addDocument(document);
            }
            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                var format = BinaryDocValuesFormat.SEPARATE_COUNT;
                assertEquals(0, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", List.of(), format)));
                assertEquals(4, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", values.subList(0, 1), format)));
                assertEquals(8, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", values.subList(1, 3), format)));
                assertEquals(12, searcher.count(new ScanningBinaryDocValuesTermInSetQuery("field", values, format)));
                assertEquals(
                    3,
                    searcher.count(
                        new ScanningBinaryDocValuesTermInSetQuery(
                            "field",
                            List.of(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::ffff:1")))),
                            format
                        )
                    )
                );
            }
        }
    }

    public void testBasics() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("a", 2L);
            expectedCounts.put("b", 5L);
            expectedCounts.put("c", 1L);
            expectedCounts.put("d", 3L);
            expectedCounts.put("e", 10L);
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (var entry : expectedCounts.entrySet()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        Document document = new Document();

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                            "field",
                            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                        );
                        field.add(new BytesRef(entry.getKey().getBytes(StandardCharsets.UTF_8)));
                        var countField = new NumericDocValuesField("field.counts", 1);

                        if (randomBoolean()) {
                            field.add(new BytesRef("z".getBytes(StandardCharsets.UTF_8)));
                            countField.setLongValue(field.count());
                        }
                        document.add(field);
                        document.add(countField);
                        writer.addDocument(document);
                    }
                }

                // search with set containing single term
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (var entry : expectedCounts.entrySet()) {
                        List<BytesRef> terms = List.of(new BytesRef(entry.getKey()));
                        long count = searcher.count(
                            new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, BinaryDocValuesFormat.SEPARATE_COUNT)
                        );
                        assertEquals(entry.getValue().longValue(), count);
                    }
                }

                // search with set containing multiple terms
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // Query for "a" or "b" should return count of a + count of b
                    List<BytesRef> terms = List.of(new BytesRef("a"), new BytesRef("b"));
                    long count = searcher.count(
                        new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, BinaryDocValuesFormat.SEPARATE_COUNT)
                    );
                    assertEquals(expectedCounts.get("a") + expectedCounts.get("b"), count);

                    // Query for "c", "d", "e" should return sum of their counts
                    List<BytesRef> terms2 = List.of(new BytesRef("c"), new BytesRef("d"), new BytesRef("e"));
                    long count2 = searcher.count(
                        new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms2, BinaryDocValuesFormat.SEPARATE_COUNT)
                    );
                    assertEquals(expectedCounts.get("c") + expectedCounts.get("d") + expectedCounts.get("e"), count2);
                }

                // search with list containing non-existent term
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    List<BytesRef> terms = List.of(new BytesRef("nonexistent"));
                    long count = searcher.count(
                        new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, BinaryDocValuesFormat.SEPARATE_COUNT)
                    );
                    assertEquals(0, count);
                }

                // search with list containing mix of existent and non-existent terms
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    List<BytesRef> terms = List.of(new BytesRef("a"), new BytesRef("nonexistent"));
                    long count = searcher.count(
                        new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms, BinaryDocValuesFormat.SEPARATE_COUNT)
                    );
                    assertEquals(expectedCounts.get("a").longValue(), count);
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";

        // no field in index
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesTermInSetQuery(
                        fieldName,
                        List.of(new BytesRef("a")),
                        BinaryDocValuesFormat.SEPARATE_COUNT
                    );
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document document = new Document();

                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    "field",
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                field.add(new BytesRef("a".getBytes(StandardCharsets.UTF_8)));
                var countField = new NumericDocValuesField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesTermInSetQuery(
                        fieldName,
                        List.of(new BytesRef("a")),
                        BinaryDocValuesFormat.SEPARATE_COUNT
                    );
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testNullTermsThrows() {
        expectThrows(
            NullPointerException.class,
            () -> new ScanningBinaryDocValuesTermInSetQuery("field", null, BinaryDocValuesFormat.SEPARATE_COUNT)
        );
    }

    public void testEqualsAndHashCode() {
        String fieldName = "field";
        List<BytesRef> terms1 = List.of(new BytesRef("a"), new BytesRef("b"));
        List<BytesRef> terms2 = List.of(new BytesRef("a"), new BytesRef("b"));
        List<BytesRef> terms3 = List.of(new BytesRef("a"), new BytesRef("c"));

        var format = BinaryDocValuesFormat.SEPARATE_COUNT;
        ScanningBinaryDocValuesTermInSetQuery query1 = new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms1, format);
        ScanningBinaryDocValuesTermInSetQuery query2 = new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms2, format);
        ScanningBinaryDocValuesTermInSetQuery query3 = new ScanningBinaryDocValuesTermInSetQuery(fieldName, terms3, format);
        ScanningBinaryDocValuesTermInSetQuery query4 = new ScanningBinaryDocValuesTermInSetQuery("other", terms1, format);

        // Same terms, same field
        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());

        // Different terms
        assertNotEquals(query1, query3);

        // Different field
        assertNotEquals(query1, query4);

        // Self equality
        assertEquals(query1, query1);
    }

    public void testDeduplication() {
        // query with duplicates should equal query without duplicates
        List<BytesRef> termsWithDuplicates = Arrays.asList(
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("a")
        );
        List<BytesRef> termsWithoutDuplicates = List.of(new BytesRef("a"), new BytesRef("b"));

        var format = BinaryDocValuesFormat.SEPARATE_COUNT;
        ScanningBinaryDocValuesTermInSetQuery query1 = new ScanningBinaryDocValuesTermInSetQuery("field", termsWithDuplicates, format);
        ScanningBinaryDocValuesTermInSetQuery query2 = new ScanningBinaryDocValuesTermInSetQuery("field", termsWithoutDuplicates, format);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

}
