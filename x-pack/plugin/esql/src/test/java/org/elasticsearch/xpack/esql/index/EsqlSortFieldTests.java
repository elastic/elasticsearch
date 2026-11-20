/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.ComputedKeySortField;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Sorts a Lucene index on {@code DATE_TRUNC(1 hour, @timestamp) DESC, rank ASC}, where the hour is computed by the ES|QL
 * evaluator from the {@code @timestamp} doc values at flush and merge time and never stored.
 */
public class EsqlSortFieldTests extends ESTestCase {

    private static final String EXPRESSION = "DATE_TRUNC(1 hour, @timestamp)";
    private static final long HOUR = 3_600_000L;
    private static final long START = 1_767_225_600_000L; // 2026-01-01T00:00:00Z
    static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
    static final Map<String, MappedFieldType> FIELDS = Map.of(
        "@timestamp",
        new DateFieldMapper.DateFieldType("@timestamp"),
        "rank",
        new NumberFieldMapper.NumberFieldType("rank", NumberFieldMapper.NumberType.LONG)
    );

    public void testFlushAndMergeSortOnComputedKey() throws IOException {
        boolean nullsFirst = randomBoolean();
        multiValued = randomBoolean();
        es819 = randomBoolean();
        Sort sort = new Sort(
            new EsqlSortField(EsqlSortKey.resolve(EXPRESSION, INDEX_SETTINGS, FIELDS::get), true, nullsFirst),
            new SortedNumericSortField("rank", SortField.Type.LONG)
        );
        try (Directory dir = newDirectory()) {
            int numDocs = 0;
            try (IndexWriter writer = new IndexWriter(dir, config(sort))) {
                int numSegments = between(3, 6);
                for (int s = 0; s < numSegments; s++) {
                    int docs = between(500, 3000);
                    for (int i = 0; i < docs; i++) {
                        writer.addDocument(randomDoc(numDocs++));
                    }
                    writer.flush();
                }
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), greaterThan(1));
                    assertSorted(reader, nullsFirst);
                }

                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.leaves().size(), equalTo(1));
                    assertThat(reader.numDocs(), equalTo(numDocs));
                    assertSorted(reader, nullsFirst);
                    // The key is computed, not stored
                    List<String> fields = new ArrayList<>();
                    for (FieldInfo fi : reader.leaves().getFirst().reader().getFieldInfos()) {
                        fields.add(fi.name);
                    }
                    assertThat(Set.copyOf(fields), equalTo(Set.of("@timestamp", "rank")));
                }
            }

            // The sort reads back from the segments through the provider in server, equal to the one built from the mapping
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    Sort readBack = leaf.reader().getMetaData().sort();
                    assertThat(readBack, equalTo(sort));
                    assertThat(readBack.getSort()[0].getClass(), equalTo(ComputedKeySortField.class));
                }
            }
            // CheckIndex can't compute the keys without the mapping, and accepts the order of the segments from that sort field on
            TestUtil.checkIndex(dir);

            // A writer reopens the index with the same sort, keeps adding segments and merges them with the existing one
            try (IndexWriter writer = new IndexWriter(dir, config(sort).setOpenMode(IndexWriterConfig.OpenMode.APPEND))) {
                for (int i = 0; i < 1000; i++) {
                    writer.addDocument(randomDoc(numDocs++));
                }
                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertThat(reader.numDocs(), equalTo(numDocs));
                    assertSorted(reader, nullsFirst);
                }
            }
        }
    }

    public void testRejectsUnsupportedKeys() {
        assertRejected("NOW()", "uses [NOW()], which isn't guaranteed to compute the same values on every node and version");
        assertRejected(
            "DATE_DIFF(\"hour\", @timestamp, NOW())",
            "uses [NOW()], which isn't guaranteed to compute the same values on every node and version"
        );
        assertRejected("MAX(rank)", "uses [MAX(rank)], which can't be computed for each document");
        assertRejected("NOPE(rank)", "uses unknown function [NOPE]");
        assertRejected("1 + 2", "must read at least one field");
        assertRejected("DATE_TRUNC(1 hour, missing)", "unknown field [missing]");
        assertRejected("CASE(rank > 0, \"a\", \"b\")", "returns type [keyword], which isn't supported yet");
        assertRejected("DATE_TRUNC(1 hour, @timestamp) DESC", "is not a single sort key");
        assertRejected("DATE_TRUNC(1 hour, @timestamp) | LIMIT 1", "is not a single sort key");
        assertRejected("DATE_TRUNC(1 hour, rank)", "second argument of [DATE_TRUNC(1 hour, rank)] must be [date_nanos or datetime]");
    }

    private static void assertRejected(String expression, String message) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlSortKey.resolve(expression, INDEX_SETTINGS, FIELDS::get)
        );
        assertThat(e.getMessage(), containsString(message));
    }

    private IndexWriterConfig config(Sort sort) {
        // A merge policy that never merges on its own keeps the flushed segments apart until the force merge
        LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(1000);
        IndexWriterConfig config = new IndexWriterConfig().setIndexSort(sort).setMergePolicy(mergePolicy);
        if (es819) {
            // Its dense numeric doc values have a bulk read
            config.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        }
        return config;
    }

    /** Single-valued segments are read in bulk, segments with documents that have several values are not. */
    private boolean multiValued;
    private boolean es819;

    /** Timestamps over a few hours, and some documents without one or, if {@link #multiValued}, with several, which have no key. */
    private Document randomDoc(int rank) {
        Document doc = new Document();
        int kind = between(0, 19);
        if (kind > 1 || (kind == 1 && multiValued == false)) {
            doc.add(new SortedNumericDocValuesField("@timestamp", START + randomLongBetween(0, 6 * HOUR)));
        } else if (kind == 1) {
            doc.add(new SortedNumericDocValuesField("@timestamp", START + randomLongBetween(0, 6 * HOUR)));
            doc.add(new SortedNumericDocValuesField("@timestamp", START + randomLongBetween(0, 6 * HOUR)));
        }
        doc.add(new SortedNumericDocValuesField("rank", rank));
        return doc;
    }

    /** Checks each segment is in key order, descending with missing keys first or last, then rank order, computed independently. */
    private static void assertSorted(DirectoryReader reader, boolean nullsFirst) throws IOException {
        Comparator<Long> keyOrder = Comparator.<Long>nullsFirst(Comparator.reverseOrder());
        if (nullsFirst == false) {
            keyOrder = Comparator.<Long>nullsLast(Comparator.reverseOrder());
        }
        for (LeafReaderContext leaf : reader.leaves()) {
            LeafReader r = leaf.reader();
            SortedNumericDocValues timestamps = r.getSortedNumericDocValues("@timestamp");
            SortedNumericDocValues ranks = r.getSortedNumericDocValues("rank");
            Long previousKey = null;
            long previousRank = Long.MIN_VALUE;
            for (int doc = 0; doc < r.maxDoc(); doc++) {
                Long key = null;
                if (timestamps.advanceExact(doc) && timestamps.docValueCount() == 1) {
                    key = Math.floorDiv(timestamps.nextValue(), HOUR) * HOUR;
                }
                assertTrue(ranks.advanceExact(doc));
                long rank = ranks.nextValue();
                if (doc > 0) {
                    int cmp = keyOrder.compare(previousKey, key);
                    assertThat("doc " + doc + " key " + key + " after " + previousKey, cmp <= 0, equalTo(true));
                    if (cmp == 0) {
                        assertThat("doc " + doc + " rank", rank, greaterThan(previousRank));
                    }
                }
                previousKey = key;
                previousRank = rank;
            }
        }
    }
}
