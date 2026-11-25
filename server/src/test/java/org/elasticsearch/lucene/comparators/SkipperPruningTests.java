/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.comparators;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class SkipperPruningTests extends ESTestCase {

    public void testCompetitiveIterator() throws IOException {

        Directory dir = newDirectory();
        Sort indexSort = new Sort(new SortField("hostname", SortField.Type.STRING), new SortField("@timestamp", SortField.Type.LONG, true));
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setIndexSort(indexSort);
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir, iwc);

        int numDocs = atLeast(1000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(SortedDocValuesField.indexedField("hostname", new BytesRef("host1")));
            doc.add(NumericDocValuesField.indexedField("@timestamp", 1_000_000 + i));
            indexWriter.addDocument(doc);
        }

        indexWriter.forceMerge(1);

        IndexReader reader = indexWriter.getReader();

        SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(
            "@timestamp",
            IndexNumericFieldData.NumericType.LONG,
            CoreValuesSourceType.NUMERIC,
            null,
            IndexType.skippers()
        );
        LongValuesComparatorSource source = new LongValuesComparatorSource(
            fd,
            null,
            MultiValueMode.MAX,
            null,
            IndexNumericFieldData.NumericType.LONG
        );

        var comparator = (XLongComparator) source.newComparator("@timestamp", 1, Pruning.GREATER_THAN_OR_EQUAL_TO, true);
        comparator.queueFull = true;
        comparator.hitsThresholdReached = true;
        comparator.bottom = Long.MAX_VALUE;
        var leafComparator = comparator.getLeafComparator(reader.leaves().getFirst());
        leafComparator.setScorer(null);
        assertThat(leafComparator.competitiveIterator().nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));

        reader.close();
        indexWriter.close();
        dir.close();
    }

    /**
     * A missing-first {@code @timestamp} sort, which is what {@code @timestamp DESC} now gives, still allows the match-tail shortcut
     * when every document has a {@code @timestamp}.
     */
    public void testMatchTailWithMissingFirstAndNoMissingValues() throws IOException {
        for (boolean usePoints : new boolean[] { false, true }) {
            int numDocs = atLeast(1000);
            assertThat(firstCompetitiveDocOfMatchTail(numDocs, 0, usePoints ? Indexing.POINTS : Indexing.SKIPPER), equalTo(numDocs - 2));
        }
    }

    /**
     * With a missing-first sort the shortcut is only taken when no document lacks a {@code @timestamp}, so otherwise the regular
     * competitive iterator is used.
     */
    public void testMatchTailWithMissingFirstAndMissingValues() throws IOException {
        for (boolean usePoints : new boolean[] { false, true }) {
            int numDocs = atLeast(1000);
            int numMissing = between(1, 10);
            int first = firstCompetitiveDocOfMatchTail(numDocs, numMissing, usePoints ? Indexing.POINTS : Indexing.SKIPPER);
            assertThat(first, not(equalTo(numDocs + numMissing - 2)));
        }
    }

    /**
     * Without a skipper or points there is no cheap way to tell that every document has a {@code @timestamp}, so a missing-first
     * sort conservatively skips the shortcut, and so any competitive iterator, even though no document is missing the field.
     */
    public void testMatchTailWithMissingFirstAndNoSkipperOrPoints() throws IOException {
        int numDocs = atLeast(1000);
        assertThat(firstCompetitiveDocOfMatchTail(numDocs, 0, Indexing.NONE), equalTo(NO_COMPETITIVE_ITERATOR));
    }

    /**
     * The queue is full after the first segment, whose timestamps are the smallest. The second segment then has no competitive
     * document, so only the tail can set the bottom again. The collector must still read only the tail of that segment.
     */
    public void testMatchTailOnSegmentAfterQueueIsFull() throws IOException {
        int numHits = between(1, 10);
        int numDocsPerSegment = atLeast(1000);
        try (Directory dir = newDirectory()) {
            SortField timestampSort = new SortField("@timestamp", SortField.Type.LONG, true);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setIndexSort(new Sort(timestampSort))
                .setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter indexWriter = new IndexWriter(dir, iwc)) {
                for (int segment = 0; segment < 2; segment++) {
                    for (int i = 0; i < numDocsPerSegment; i++) {
                        Document doc = new Document();
                        doc.add(NumericDocValuesField.indexedField("@timestamp", 1_000_000L * (segment + 1) + i));
                        indexWriter.addDocument(doc);
                    }
                    indexWriter.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), equalTo(2));
                SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(
                    "@timestamp",
                    IndexNumericFieldData.NumericType.LONG,
                    CoreValuesSourceType.NUMERIC,
                    null,
                    IndexType.skippers()
                );
                LongValuesComparatorSource source = new LongValuesComparatorSource(
                    fd,
                    null,
                    MultiValueMode.MIN,
                    null,
                    IndexNumericFieldData.NumericType.LONG
                );
                source.setMatchTailQuery();
                Sort sort = new Sort(new SortField("@timestamp", source, false));
                TopFieldCollector collector = new TopFieldCollectorManager(sort, numHits, null, 0).newCollector();
                int[] collectedPerLeaf = new int[2];
                new IndexSearcher(reader).search(new MatchAllDocsQuery(), countingCollector(collector, collectedPerLeaf));
                assertThat(collector.topDocs().scoreDocs.length, equalTo(numHits));
                assertThat(collectedPerLeaf[1], lessThanOrEqualTo(numHits + 1));
                // The tail is set before any hit, so the first segment also reads only its tail
                assertThat(collectedPerLeaf[0], lessThanOrEqualTo(numHits + 1));
            }
        }
    }

    /** Counts the documents that each leaf collects, and keeps the competitive iterator of the collector. */
    private static Collector countingCollector(Collector in, int[] collectedPerLeaf) {
        return new FilterCollector(in) {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                LeafCollector leaf = super.getLeafCollector(context);
                return new FilterLeafCollector(leaf) {
                    @Override
                    public void collect(int doc) throws IOException {
                        collectedPerLeaf[context.ord]++;
                        super.collect(doc);
                    }

                    @Override
                    public DocIdSetIterator competitiveIterator() throws IOException {
                        return leaf.competitiveIterator();
                    }
                };
            }
        };
    }

    private static final int NO_COMPETITIVE_ITERATOR = -1;

    /** How the {@code @timestamp} field is indexed, which decides whether the segment can tell cheaply that it is dense. */
    private enum Indexing {
        SKIPPER,
        POINTS,
        NONE
    }

    /**
     * Returns the first doc of the competitive iterator for a single-hit match-tail sort on a segment sorted by
     * {@code @timestamp} descending with missing values first.
     */
    private int firstCompetitiveDocOfMatchTail(int numDocsWithTimestamp, int numDocsWithoutTimestamp, Indexing indexing)
        throws IOException {
        try (Directory dir = newDirectory()) {
            SortField timestampSort = new SortField("@timestamp", SortField.Type.LONG, true, Long.MAX_VALUE);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random())).setIndexSort(new Sort(timestampSort));
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), dir, iwc)) {
                for (int i = 0; i < numDocsWithTimestamp; i++) {
                    Document doc = new Document();
                    switch (indexing) {
                        case POINTS -> {
                            doc.add(new NumericDocValuesField("@timestamp", 1_000_000 + i));
                            doc.add(new LongPoint("@timestamp", 1_000_000 + i));
                        }
                        case SKIPPER -> doc.add(NumericDocValuesField.indexedField("@timestamp", 1_000_000 + i));
                        case NONE -> doc.add(new NumericDocValuesField("@timestamp", 1_000_000 + i));
                    }
                    indexWriter.addDocument(doc);
                }
                for (int i = 0; i < numDocsWithoutTimestamp; i++) {
                    Document doc = new Document();
                    doc.add(new NumericDocValuesField("other", i));
                    indexWriter.addDocument(doc);
                }
                indexWriter.forceMerge(1);
                try (IndexReader reader = indexWriter.getReader()) {
                    SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(
                        "@timestamp",
                        IndexNumericFieldData.NumericType.LONG,
                        CoreValuesSourceType.NUMERIC,
                        null,
                        switch (indexing) {
                            case POINTS -> IndexType.points(true, true);
                            case SKIPPER -> IndexType.skippers();
                            case NONE -> IndexType.NONE;
                        }
                    );
                    LongValuesComparatorSource source = new LongValuesComparatorSource(
                        fd,
                        null,
                        MultiValueMode.MIN,
                        null,
                        IndexNumericFieldData.NumericType.LONG
                    );
                    source.setMatchTailQuery();
                    // Ascending: the oldest documents are at the tail of a segment sorted by descending @timestamp
                    var comparator = (XLongComparator) source.newComparator("@timestamp", 1, Pruning.GREATER_THAN_OR_EQUAL_TO, false);
                    comparator.queueFull = true;
                    comparator.hitsThresholdReached = true;
                    comparator.bottom = Long.MIN_VALUE;
                    var leafComparator = comparator.getLeafComparator(reader.leaves().getFirst());
                    leafComparator.setScorer(null);
                    // The iterator only narrows to the tail once the hits threshold is reached
                    leafComparator.setHitsThresholdReached();
                    var iterator = leafComparator.competitiveIterator();
                    return iterator == null ? NO_COMPETITIVE_ITERATOR : iterator.nextDoc();
                }
            }
        }
    }
}
