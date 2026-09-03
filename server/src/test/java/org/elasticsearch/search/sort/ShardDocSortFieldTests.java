/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@code _shard_doc} is {@code _doc} plus a shard prefix. These tests check that Lucene
 * can still skip non-competitive docs on search_after, and that the shard-prefix remapping
 * of the cursor is correct when pruning is enabled.
 */
public class ShardDocSortFieldTests extends ESTestCase {

    public void testCompetitiveIteratorSkipsToSearchAfterOnSameShard() throws IOException {
        int numDocs = atLeast(50);
        int afterDoc = randomIntBetween(1, numDocs - 2);
        int shardIndex = randomIntBetween(0, 10);
        withSingleSegmentReader(numDocs, reader -> {
            LeafFieldComparator leaf = competitiveLeaf(
                new ShardDocSortField(shardIndex, false),
                Pruning.GREATER_THAN_OR_EQUAL_TO,
                ShardDocSortField.encodeShardAndDoc(shardIndex, afterDoc),
                reader.leaves().getFirst()
            );
            assertThat(leaf.competitiveIterator().nextDoc(), equalTo(afterDoc));
        });
    }

    public void testCompetitiveIteratorSkipsCompletedShard() throws IOException {
        int numDocs = atLeast(20);
        int shardIndex = randomIntBetween(0, 10);
        withSingleSegmentReader(numDocs, reader -> {
            LeafFieldComparator leaf = competitiveLeaf(
                new ShardDocSortField(shardIndex, false),
                Pruning.GREATER_THAN_OR_EQUAL_TO,
                ShardDocSortField.encodeShardAndDoc(shardIndex + 1, 0),
                reader.leaves().getFirst()
            );
            assertThat(leaf.competitiveIterator().nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
        });
    }

    public void testCompetitiveIteratorDoesNotSkipLaterShard() throws IOException {
        int numDocs = atLeast(20);
        int shardIndex = randomIntBetween(1, 10);
        withSingleSegmentReader(numDocs, reader -> {
            LeafFieldComparator leaf = competitiveLeaf(
                new ShardDocSortField(shardIndex, false),
                Pruning.GREATER_THAN_OR_EQUAL_TO,
                ShardDocSortField.encodeShardAndDoc(shardIndex - 1, randomIntBetween(0, numDocs - 1)),
                reader.leaves().getFirst()
            );
            assertThat(leaf.competitiveIterator().nextDoc(), equalTo(0));
        });
    }

    public void testCompetitiveIteratorMapsCursorOntoEachSegment() throws IOException {
        int numDocs = atLeast(40);
        int shardIndex = randomIntBetween(0, 10);
        withMultiSegmentReader(numDocs, randomIntBetween(2, 5), reader -> {
            List<LeafReaderContext> leaves = reader.leaves();
            // pick a cursor inside a segment other than the first, so that both branches below are taken
            LeafReaderContext cursorLeaf = leaves.get(randomIntBetween(1, leaves.size() - 1));
            int afterDoc = cursorLeaf.docBase + randomIntBetween(0, cursorLeaf.reader().maxDoc() - 1);
            for (LeafReaderContext context : leaves) {
                LeafFieldComparator leaf = competitiveLeaf(
                    new ShardDocSortField(shardIndex, false),
                    Pruning.GREATER_THAN_OR_EQUAL_TO,
                    ShardDocSortField.encodeShardAndDoc(shardIndex, afterDoc),
                    context
                );
                int firstCompetitive = leaf.competitiveIterator().nextDoc();
                if (context.docBase + context.reader().maxDoc() <= afterDoc) {
                    // the cursor is past this segment, so none of its docs can compete
                    assertThat(firstCompetitive, equalTo(DocIdSetIterator.NO_MORE_DOCS));
                } else {
                    // the cursor maps to a segment local doc id, and clamps to 0 for segments after it
                    assertThat(firstCompetitive, equalTo(Math.max(0, afterDoc - context.docBase)));
                }
            }
        });
    }

    public void testPruningNoneDisablesCompetitiveIterator() throws IOException {
        int numDocs = atLeast(10);
        withSingleSegmentReader(numDocs, reader -> {
            LeafFieldComparator leaf = competitiveLeaf(
                new ShardDocSortField(0, false),
                Pruning.NONE,
                ShardDocSortField.encodeShardAndDoc(0, 1),
                reader.leaves().getFirst()
            );
            assertThat(leaf.competitiveIterator(), nullValue());
        });
    }

    public void testReverseSortDisablesCompetitiveIterator() throws IOException {
        int numDocs = atLeast(10);
        withSingleSegmentReader(numDocs, reader -> {
            LeafFieldComparator leaf = competitiveLeaf(
                new ShardDocSortField(0, true),
                Pruning.GREATER_THAN_OR_EQUAL_TO,
                ShardDocSortField.encodeShardAndDoc(0, numDocs - 1),
                reader.leaves().getFirst()
            );
            assertThat(leaf.competitiveIterator(), nullValue());
        });
    }

    public void testSearchAfterWalksAllDocs() throws IOException {
        int numDocs = scaledRandomIntBetween(50, 200);
        int pageSize = randomIntBetween(5, 20);
        withSingleSegmentReader(numDocs, reader -> assertSearchAfterWalksAllDocs(reader, pageSize));
    }

    public void testSearchAfterWalksAllDocsAcrossSegments() throws IOException {
        int numDocs = scaledRandomIntBetween(50, 200);
        int pageSize = randomIntBetween(5, 20);
        withMultiSegmentReader(numDocs, randomIntBetween(2, 5), reader -> assertSearchAfterWalksAllDocs(reader, pageSize));
    }

    public void testSearchAfterOnLaterShardReturnsNoHits() throws IOException {
        int numDocs = atLeast(20);
        withSingleSegmentReader(numDocs, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            Sort sort = new Sort(new ShardDocSortField(0, false));
            FieldDoc after = new FieldDoc(0, Float.NaN, new Object[] { ShardDocSortField.encodeShardAndDoc(1, 0) });
            TopFieldDocs topDocs = searcher.search(MatchAllDocsQuery.INSTANCE, new TopFieldCollectorManager(sort, 10, after, 1));
            assertThat(topDocs.scoreDocs.length, equalTo(0));
        });
    }

    public void testSearchAfterOnEarlierShardStartsAtFirstDoc() throws IOException {
        int numDocs = atLeast(20);
        withSingleSegmentReader(numDocs, reader -> {
            IndexSearcher searcher = new IndexSearcher(reader);
            Sort sort = new Sort(new ShardDocSortField(1, false));
            FieldDoc after = new FieldDoc(5, Float.NaN, new Object[] { ShardDocSortField.encodeShardAndDoc(0, 5) });
            TopFieldDocs topDocs = searcher.search(MatchAllDocsQuery.INSTANCE, new TopFieldCollectorManager(sort, 10, after, 1));
            assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
            assertThat(((FieldDoc) topDocs.scoreDocs[0]).fields[0], equalTo(ShardDocSortField.encodeShardAndDoc(1, 0)));
        });
    }

    /**
     * Pages through the whole reader with search_after and checks that pruning neither loses nor repeats a hit.
     */
    private static void assertSearchAfterWalksAllDocs(IndexReader reader, int pageSize) throws IOException {
        int numDocs = reader.numDocs();
        IndexSearcher searcher = new IndexSearcher(reader);
        Sort sort = new Sort(new ShardDocSortField(0, false));
        FieldDoc after = null;
        int seen = 0;
        int lastDoc = -1;
        while (seen < numDocs) {
            TopFieldDocs topDocs = searcher.search(MatchAllDocsQuery.INSTANCE, new TopFieldCollectorManager(sort, pageSize, after, 1));
            assertThat(topDocs.scoreDocs.length, greaterThan(0));
            // pruning stops the scan once the queue is full, so we don't count every match
            assertThat(topDocs.totalHits.value(), lessThan((long) numDocs));
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                assertThat(scoreDoc.doc, greaterThan(lastDoc));
                lastDoc = scoreDoc.doc;
                FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                assertThat(fieldDoc.fields[0], equalTo(ShardDocSortField.encodeShardAndDoc(0, scoreDoc.doc)));
                after = fieldDoc;
                seen++;
            }
            if (topDocs.scoreDocs.length < pageSize) {
                break;
            }
        }
        assertThat(seen, equalTo(numDocs));
    }

    private static void withSingleSegmentReader(int numDocs, CheckedConsumer<IndexReader, IOException> consumer) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(new Document());
                }
                writer.forceMerge(1);
                try (IndexReader reader = writer.getReader()) {
                    assertThat(reader.leaves().size(), equalTo(1));
                    consumer.accept(reader);
                }
            }
        }
    }

    /**
     * Builds a reader with at least {@code segments} leaves. Pruning maps the search_after cursor onto every segment
     * separately using its {@code docBase}, and keeps skipping later segments once the queue is full, so a
     * single segment index leaves that arithmetic untested.
     */
    private static void withMultiSegmentReader(int numDocs, int segments, CheckedConsumer<IndexReader, IOException> consumer)
        throws IOException {
        assert numDocs > segments && segments > 1;
        // NoMergePolicy keeps the flushed segments apart, and also disables RandomIndexWriter's random force merge
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config)) {
                int docsPerSegment = numDocs / segments;
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(new Document());
                    if ((i + 1) % docsPerSegment == 0) {
                        writer.flush();
                    }
                }
                try (IndexReader reader = writer.getReader()) {
                    assertThat(reader.numDocs(), equalTo(numDocs));
                    assertThat(reader.leaves().size(), greaterThanOrEqualTo(segments));
                    consumer.accept(reader);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static LeafFieldComparator competitiveLeaf(
        ShardDocSortField sortField,
        Pruning pruning,
        long topValue,
        LeafReaderContext context
    ) throws IOException {
        FieldComparator<Long> comparator = (FieldComparator<Long>) sortField.getComparator(10, pruning);
        comparator.setTopValue(topValue);
        LeafFieldComparator leaf = comparator.getLeafComparator(context);
        leaf.setHitsThresholdReached();
        return leaf;
    }
}
