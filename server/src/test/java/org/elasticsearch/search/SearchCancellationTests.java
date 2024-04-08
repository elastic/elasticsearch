/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class SearchCancellationTests extends ESTestCase {

    private static final String STRING_FIELD_NAME = "foo";
    private static final String POINT_FIELD_NAME = "point";
    private static final String KNN_FIELD_NAME = "vector";

    private static Directory dir;
    private static IndexReader reader;

    @BeforeClass
    public static void setup() throws IOException {
        dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        // we need at least 2 segments - so no merges should be allowed
        w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        w.setDoRandomForceMerge(false);
        indexRandomDocuments(w, TestUtil.nextInt(random(), 2, 20));
        w.flush();
        indexRandomDocuments(w, TestUtil.nextInt(random(), 1, 20));
        reader = w.getReader();
        w.close();
    }

    private static void indexRandomDocuments(RandomIndexWriter w, int numDocs) throws IOException {
        for (int i = 1; i <= numDocs; ++i) {
            Document doc = new Document();
            doc.add(new StringField(STRING_FIELD_NAME, "a".repeat(i), Field.Store.NO));
            doc.add(new IntPoint(POINT_FIELD_NAME, i, i + 1));
            doc.add(new KnnFloatVectorField(KNN_FIELD_NAME, new float[] { 1.0f, 0.5f, 42.0f }));
            w.addDocument(doc);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        IOUtils.close(reader, dir);
        dir = null;
        reader = null;
    }

    public void testAddingCancellationActions() throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );
        NullPointerException npe = expectThrows(NullPointerException.class, () -> searcher.addQueryCancellation(null));
        assertEquals("cancellation runnable should not be null", npe.getMessage());

        Runnable r = () -> {};
        searcher.addQueryCancellation(r);
    }

    public void testCancellableCollector() throws IOException {
        TotalHitCountCollector collector1 = new TotalHitCountCollector();
        Runnable cancellation = () -> { throw new TaskCancelledException("cancelled"); };
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );

        searcher.search(new MatchAllDocsQuery(), collector1);
        assertThat(collector1.getTotalHits(), equalTo(reader.numDocs()));

        searcher.addQueryCancellation(cancellation);
        expectThrows(TaskCancelledException.class, () -> searcher.search(new MatchAllDocsQuery(), collector1));

        searcher.removeQueryCancellation(cancellation);
        TotalHitCountCollector collector2 = new TotalHitCountCollector();
        searcher.search(new MatchAllDocsQuery(), collector2);
        assertThat(collector2.getTotalHits(), equalTo(reader.numDocs()));
    }

    public void testExitableDirectoryReader() throws IOException {
        AtomicBoolean cancelled = new AtomicBoolean(true);
        Runnable cancellation = () -> {
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
        };
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );
        searcher.addQueryCancellation(cancellation);
        CompiledAutomaton automaton = new CompiledAutomaton(new RegExp("a.*").toAutomaton());

        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().terms(STRING_FIELD_NAME).iterator()
        );
        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().terms(STRING_FIELD_NAME).intersect(automaton, null)
        );
        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME)
        );
        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME)
        );

        cancelled.set(false); // Avoid exception during construction of the wrapper objects
        Terms terms = searcher.getIndexReader().leaves().get(0).reader().terms(STRING_FIELD_NAME);
        TermsEnum termsIterator = terms.iterator();
        TermsEnum termsIntersect = terms.intersect(automaton, null);
        PointValues pointValues1 = searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME);
        cancelled.set(true);
        expectThrows(TaskCancelledException.class, termsIterator::next);
        expectThrows(TaskCancelledException.class, termsIntersect::next);
        expectThrows(TaskCancelledException.class, pointValues1::getDocCount);
        expectThrows(TaskCancelledException.class, pointValues1::getNumIndexDimensions);
        expectThrows(TaskCancelledException.class, () -> pointValues1.intersect(new PointValuesIntersectVisitor()));

        cancelled.set(false); // Avoid exception during construction of the wrapper objects
        // Re-initialize objects so that we reset the `calls` counter used to avoid cancellation check
        // on every iteration and assure that cancellation would normally happen if we hadn't removed the
        // cancellation runnable.
        termsIterator = terms.iterator();
        termsIntersect = terms.intersect(automaton, null);
        PointValues pointValues2 = searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME);
        cancelled.set(true);
        searcher.removeQueryCancellation(cancellation);
        termsIterator.next();
        termsIntersect.next();
        pointValues2.getDocCount();
        pointValues2.getNumIndexDimensions();
        pointValues2.intersect(new PointValuesIntersectVisitor());
    }

    public void testExitableDirectoryReaderVectors() throws IOException {
        AtomicBoolean cancelled = new AtomicBoolean(true);
        Runnable cancellation = () -> {
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
        };
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true
        );
        searcher.addQueryCancellation(cancellation);
        final LeafReader leaf = searcher.getIndexReader().leaves().get(0).reader();
        expectThrows(TaskCancelledException.class, () -> leaf.getFloatVectorValues(KNN_FIELD_NAME));
        expectThrows(
            TaskCancelledException.class,
            () -> leaf.searchNearestVectors(KNN_FIELD_NAME, new float[] { 1f, 1f, 1f }, 2, leaf.getLiveDocs(), Integer.MAX_VALUE)
        );

        cancelled.set(false); // Avoid exception during construction of the wrapper objects
        FloatVectorValues vectorValues = searcher.getIndexReader().leaves().get(0).reader().getFloatVectorValues(KNN_FIELD_NAME);
        cancelled.set(true);
        // On the first doc when already canceled, it throws
        expectThrows(TaskCancelledException.class, vectorValues::nextDoc);

        cancelled.set(false); // Avoid exception during construction of the wrapper objects
        FloatVectorValues uncancelledVectorValues = searcher.getIndexReader().leaves().get(0).reader().getFloatVectorValues(KNN_FIELD_NAME);
        cancelled.set(true);
        searcher.removeQueryCancellation(cancellation);
        // On the first doc when already canceled, it throws, but with the cancellation removed, it should not
        uncancelledVectorValues.nextDoc();
    }

    private static class PointValuesIntersectVisitor implements PointValues.IntersectVisitor {
        @Override
        public void visit(int docID) {}

        @Override
        public void visit(int docID, byte[] packedValue) {}

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }
}
