/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesPrefixQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesRangeQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermInSetQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermQuery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * The filters of the binary doc values ({@code tryTermEqualIterator} and {@code tryContainsIterator}) must find the documents that a
 * scan of the values finds, for any column (with a dictionary, without one, and with a dictionary that the writer gave up on in the
 * middle of a chunk), for columns with and without missing values, for any term, and for any window of the documents.
 */
public class ParquetBinaryFilterTests extends StubSegmentTestCase {

    private static final List<ColumnBinding> BINDINGS = List.of(
        new ColumnBinding("f.req", 0, "req", DocValuesType.BINARY),
        new ColumnBinding("f.opt", 1, "opt", DocValuesType.BINARY)
    );

    private enum Filter {
        EQUALS,
        CONTAINS,
        /** PROTOTYPE: the general hook, with the matcher of a prefix query. */
        PREFIX;

        DocIdSetIterator iterator(BinaryDocValues values, BytesRef term) throws IOException {
            assertTrue("the iterator filters", values instanceof BlockLoader.OptionalColumnAtATimeReader);
            BlockLoader.OptionalColumnAtATimeReader reader = (BlockLoader.OptionalColumnAtATimeReader) values;
            return switch (this) {
                case EQUALS -> reader.tryTermEqualIterator(term);
                case CONTAINS -> reader.tryContainsIterator(term);
                case PREFIX -> reader.tryMatchIterator(
                    value -> value.length >= term.length
                        && Arrays.equals(
                            value.bytes,
                            value.offset,
                            value.offset + term.length,
                            term.bytes,
                            term.offset,
                            term.offset + term.length
                        )
                );
            };
        }

        Predicate<String> expected(String term) {
            return switch (this) {
                case EQUALS -> value -> value.equals(term);
                case CONTAINS -> value -> value.contains(term);
                case PREFIX -> value -> value.startsWith(term);
            };
        }
    }

    /** The documents that a scan of the values finds. */
    private static List<Integer> scan(LeafReader leaf, String field, Predicate<String> matcher) throws IOException {
        BinaryDocValues values = leaf.getBinaryDocValues(field);
        List<Integer> docs = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            BytesRef value = values.binaryValue();
            if (matcher.test(new String(value.bytes, value.offset, value.length, StandardCharsets.UTF_8))) {
                docs.add(doc);
            }
        }
        return docs;
    }

    private static List<Integer> collect(DocIdSetIterator iterator, int from, int to) throws IOException {
        List<Integer> docs = new ArrayList<>();
        for (int doc = from == 0 ? iterator.nextDoc() : iterator.advance(from); doc < to; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private static void assertFilters(LeafReader leaf, String field, int rows, List<String> terms) throws IOException {
        for (Filter filter : Filter.values()) {
            for (String term : terms) {
                List<Integer> expected = scan(leaf, field, filter.expected(term));
                BytesRef bytes = new BytesRef(term);
                DocIdSetIterator all = filter.iterator(leaf.getBinaryDocValues(field), bytes);
                assertNotNull(
                    "a filter is a two phase iterator, so that a window of documents costs what it holds",
                    TwoPhaseIterator.unwrap(all)
                );
                assertEquals(filter + " [" + term + "] on " + field, expected, collect(all, 0, DocIdSetIterator.NO_MORE_DOCS));
                assertEquals("an exhausted iterator stays so", DocIdSetIterator.NO_MORE_DOCS, all.nextDoc());

                // a window, as a slice of the documents of a segment: the documents of the window, and nothing before or after it
                int from = randomIntBetween(0, rows);
                int to = randomIntBetween(from, rows);
                List<Integer> window = expected.stream().filter(doc -> doc >= from && doc < to).toList();
                DocIdSetIterator sliced = filter.iterator(leaf.getBinaryDocValues(field), bytes);
                assertEquals(filter + " [" + term + "] in [" + from + ", " + to + ")", window, collect(sliced, from, to));

                // jumps forward by random distances
                DocIdSetIterator jumping = filter.iterator(leaf.getBinaryDocValues(field), bytes);
                int target = 0;
                while (target < rows) {
                    final int jumpTarget = target;
                    int doc = jumping.advance(jumpTarget);
                    int firstAtOrAfter = expected.stream().filter(d -> d >= jumpTarget).findFirst().orElse(DocIdSetIterator.NO_MORE_DOCS);
                    assertEquals(filter + " [" + term + "] advance(" + jumpTarget + ")", firstAtOrAfter, doc);
                    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                        break;
                    }
                    target = doc + 1 + randomIntBetween(0, 40);
                }
            }
        }
    }

    private static List<String> termsFor(List<String> values) {
        List<String> terms = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String value = randomFrom(values);
            if (value != null) {
                terms.add(value);
                if (value.length() > 2) {
                    int start = randomIntBetween(0, value.length() - 1);
                    terms.add(value.substring(start, randomIntBetween(start + 1, value.length())));
                }
            }
        }
        terms.add("no-such-value");
        terms.add("");
        terms.add("é");
        return terms;
    }

    public void testFiltersFindWhatAScanFindsForColumnsOfAnyCardinality() throws IOException {
        int rows = randomIntBetween(1, 1_500);
        int distinct = randomFrom(1, 3, 20, rows);
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            // the second half has only distinct values, so a writer with a small dictionary falls back to plain pages in the middle
            int id = i < rows / 2 ? i % distinct : i;
            required.add("req-" + id + (id % 7 == 0 ? "-é" : ""));
            optional.add(i % 4 == 0 ? null : "opt-" + (id % 11));
        }
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(
            randomBoolean(),
            randomFrom(64, 256, 4096),
            randomFrom(64, 256, 1024 * 1024),
            randomBoolean() ? CompressionCodecName.SNAPPY : CompressionCodecName.UNCOMPRESSED
        );
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeStrings(required, optional, options));
        withSegment(file, BINDINGS, leaf -> {
            assertFilters(leaf, "f.req", rows, termsFor(required));
            assertFilters(leaf, "f.opt", rows, termsFor(optional));
        });
    }

    public void testTheFilterOfOneIteratorDoesNotMoveTheValuesThatItCameFrom() throws IOException {
        int rows = randomIntBetween(10, 500);
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            required.add("req-" + i % 6);
            optional.add("opt-" + i % 6);
        }
        BytesParquetObject file = new BytesParquetObject(
            FILE,
            ParquetTestFiles.writeStrings(
                required,
                optional,
                new ParquetTestFiles.Options(true, 256, 1024 * 1024, CompressionCodecName.SNAPPY)
            )
        );
        withSegment(file, BINDINGS, leaf -> {
            BinaryDocValues values = leaf.getBinaryDocValues("f.req");
            int doc = values.advance(rows / 2);
            DocIdSetIterator filter = Filter.EQUALS.iterator(values, new BytesRef("req-3"));
            collect(filter, 0, DocIdSetIterator.NO_MORE_DOCS);
            assertEquals(doc, values.docID());
            assertEquals(required.get(doc), values.binaryValue().utf8ToString());
        });
    }

    public void testAnExactTermQueryOfTheSearchLayerUsesTheFilter() throws IOException {
        int rows = randomIntBetween(1, 1_000);
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            required.add("req-" + i % 17);
            optional.add(null);
        }
        BytesParquetObject file = new BytesParquetObject(
            FILE,
            ParquetTestFiles.writeStrings(required, optional, ParquetTestFiles.Options.DEFAULT)
        );
        withSegment(file, BINDINGS, leaf -> {
            String term = randomFrom(required);
            IndexSearcher searcher = new IndexSearcher(leaf);
            int count = searcher.count(
                new ScanningBinaryDocValuesTermQuery("f.req", new BytesRef(term), BinaryDocValuesFormat.SEPARATE_COUNT)
            );
            assertEquals(required.stream().filter(term::equals).count(), count);
        });
    }

    /** PROTOTYPE: the queries that have no hook of their own reach the general one, and find what a scan finds. */
    public void testPrefixRangeAndInSetQueriesOfTheSearchLayerFindWhatAScanFinds() throws IOException {
        int rows = randomIntBetween(1, 1_000);
        int distinct = randomFrom(2, 17, rows);
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            required.add("req-" + i % distinct);
            optional.add(null);
        }
        ParquetTestFiles.Options options = new ParquetTestFiles.Options(
            randomBoolean(),
            randomFrom(64, 4096),
            randomFrom(256, 1024 * 1024),
            CompressionCodecName.UNCOMPRESSED
        );
        BytesParquetObject file = new BytesParquetObject(FILE, ParquetTestFiles.writeStrings(required, optional, options));
        withSegment(file, BINDINGS, leaf -> {
            IndexSearcher searcher = new IndexSearcher(leaf);
            BinaryDocValuesFormat format = BinaryDocValuesFormat.SEPARATE_COUNT;
            String prefix = "req-" + randomIntBetween(0, 9);
            assertEquals(
                required.stream().filter(v -> v.startsWith(prefix)).count(),
                searcher.count(new ScanningBinaryDocValuesPrefixQuery("f.req", prefix, false, format))
            );
            String lower = randomFrom(required);
            String upper = randomFrom(required);
            assertEquals(
                required.stream().filter(v -> v.compareTo(lower) >= 0 && v.compareTo(upper) <= 0).count(),
                searcher.count(new ScanningBinaryDocValuesRangeQuery("f.req", new BytesRef(lower), new BytesRef(upper), format))
            );
            List<String> terms = List.of(randomFrom(required), randomFrom(required), "no-such-value");
            assertEquals(
                required.stream().filter(terms::contains).count(),
                searcher.count(new ScanningBinaryDocValuesTermInSetQuery("f.req", terms.stream().map(BytesRef::new).toList(), format))
            );
        });
    }

    public void testAPageThatCannotMatchIsNotDecodedWhenTheDictionaryHasNoMatchingEntry() throws IOException {
        BytesPageMatcher matcher = new BytesPageMatcher(new BytesRef("b"), BytesPageMatcher.Mode.EQUALS);
        matcher.startChunk();
        assertFalse("no dictionary yet, so a page must be read", matcher.skipPage(Encoding.RLE_DICTIONARY));
        // a dictionary of "a", "c": plain values, each a 4 byte little endian length and the bytes
        byte[] dictionary = { 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'c' };
        matcher.dictionary(dictionary, 0, dictionary.length, 2);
        assertTrue("no entry is equal to the term", matcher.skipPage(Encoding.RLE_DICTIONARY));
        assertTrue(matcher.skipPage(Encoding.PLAIN_DICTIONARY));
        assertFalse("a plain page may hold the term whatever the dictionary has", matcher.skipPage(Encoding.PLAIN));
        assertFalse(matcher.pageHasMatch());

        BytesPageMatcher contains = new BytesPageMatcher(new BytesRef("c"), BytesPageMatcher.Mode.CONTAINS);
        contains.startChunk();
        contains.dictionary(dictionary, 0, dictionary.length, 2);
        assertFalse("an entry has the term", contains.skipPage(Encoding.RLE_DICTIONARY));

        matcher.startChunk();
        assertFalse("the dictionary of the previous chunk does not apply to the next one", matcher.skipPage(Encoding.RLE_DICTIONARY));
    }
}
