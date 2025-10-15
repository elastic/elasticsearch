/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.lucene.document.NumericField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;

/**
 *  Mostly copied from lucene test code, but adapted to use with {@link ContextIndexSearcher}.
 */
public class MaxClauseLimitTests extends ESTestCase {

    public void testFlattenInnerDisjunctionsWithMoreThan1024Terms() throws IOException {
        IndexSearcher searcher = newContextIndexSearcher(new MultiReader());

        BooleanQuery.Builder builder1024 = new BooleanQuery.Builder();
        for (int i = 0; i < 1024; i++) {
            builder1024.add(new TermQuery(new Term("foo", "bar-" + i)), BooleanClause.Occur.SHOULD);
        }
        Query inner = builder1024.build();
        Query query = new BooleanQuery.Builder().add(inner, BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("foo", "baz")), BooleanClause.Occur.SHOULD)
            .build();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { searcher.rewrite(query); });
        assertFalse(
            "Should have been caught during flattening and not required full nested walk",
            ex.getCause() instanceof IndexSearcher.TooManyNestedClauses
        );
    }

    public void testLargeTermsNestedFirst() throws IOException {
        IndexSearcher searcher = newContextIndexSearcher(new MultiReader());
        BooleanQuery.Builder nestedBuilder = new BooleanQuery.Builder();

        nestedBuilder.setMinimumNumberShouldMatch(5);

        for (int i = 0; i < 600; i++) {
            nestedBuilder.add(new TermQuery(new Term("foo", "bar-" + i)), BooleanClause.Occur.SHOULD);
        }
        Query inner = nestedBuilder.build();
        BooleanQuery.Builder builderMixed = new BooleanQuery.Builder().add(inner, BooleanClause.Occur.SHOULD);

        builderMixed.setMinimumNumberShouldMatch(5);

        for (int i = 0; i < 600; i++) {
            builderMixed.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
        }

        Query query = builderMixed.build();

        // Can't be flattened, but high clause count should still be cause during nested walk...
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { searcher.rewrite(query); });
        assertThat(ex.getCause(), instanceOf(IndexSearcher.TooManyNestedClauses.class));
    }

    public void testLargeTermsNestedLast() throws IOException {
        IndexSearcher searcher = newContextIndexSearcher(new MultiReader());
        BooleanQuery.Builder nestedBuilder = new BooleanQuery.Builder();

        nestedBuilder.setMinimumNumberShouldMatch(5);

        for (int i = 0; i < 600; i++) {
            nestedBuilder.add(new TermQuery(new Term("foo", "bar-" + i)), BooleanClause.Occur.SHOULD);
        }
        Query inner = nestedBuilder.build();
        BooleanQuery.Builder builderMixed = new BooleanQuery.Builder();

        builderMixed.setMinimumNumberShouldMatch(5);

        for (int i = 0; i < 600; i++) {
            builderMixed.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.SHOULD);
        }

        builderMixed.add(inner, BooleanClause.Occur.SHOULD);

        Query query = builderMixed.build();

        // Can't be flattened, but high clause count should still be cause during nested walk...
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { searcher.rewrite(query); });
        assertThat(ex.getCause(), instanceOf(IndexSearcher.TooManyNestedClauses.class));
    }

    public void testLargeDisjunctionMaxQuery() throws IOException {
        IndexSearcher searcher = newContextIndexSearcher(new MultiReader());
        Query[] clausesQueryArray = new Query[1050];

        for (int i = 0; i < 1049; i++) {
            clausesQueryArray[i] = new TermQuery(new Term("field", "a"));
        }

        PhraseQuery pq = new PhraseQuery("field", new String[0]);

        clausesQueryArray[1049] = pq;

        DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(Arrays.asList(clausesQueryArray), 0.5f);

        // Can't be flattened, but high clause count should still be cause during nested walk...
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { searcher.rewrite(dmq); });
        assertThat(ex.getCause(), instanceOf(IndexSearcher.TooManyNestedClauses.class));
    }

    public void testMultiExactWithRepeats() throws IOException {
        IndexSearcher searcher = newContextIndexSearcher(new MultiReader());
        MultiPhraseQuery.Builder qb = new MultiPhraseQuery.Builder();

        for (int i = 0; i < 1050; i++) {
            qb.add(new Term[] { new Term("foo", "bar-" + i), new Term("foo", "bar+" + i) }, 0);
        }

        // Can't be flattened, but high clause count should still be cause during nested walk...
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { searcher.rewrite(qb.build()); });
        assertThat(ex.getCause(), instanceOf(IndexSearcher.TooManyNestedClauses.class));
    }

    public void testIndexOrDocValues() throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
        Document d = new Document();
        d.add(new LongField("foo", 0L, LongField.Store.NO));
        writer.addDocument(d);
        d = new Document();
        d.add(new LongField("foo", Long.MAX_VALUE, LongField.Store.NO));
        writer.addDocument(d);

        IndexReader reader = writer.getReader();
        IndexSearcher searcher = newContextIndexSearcher(reader);
        writer.close();
        int maxClauseCount = IndexSearcher.getMaxClauseCount();
        BooleanQuery.Builder qb = new BooleanQuery.Builder();

        for (int i = 0; i < maxClauseCount; i++) {
            qb.add(LongPoint.newRangeQuery("foo", 0, i), BooleanClause.Occur.SHOULD);
        }
        // should not throw an exception, because it is below the limit
        searcher.rewrite(qb.build());

        qb = new BooleanQuery.Builder();
        for (int i = 0; i < maxClauseCount; i++) {
            qb.add(NumericField.newRangeLongQuery("foo", 0, i), BooleanClause.Occur.SHOULD);
        }
        // should not throw an exception, because it is below the limit
        searcher.rewrite(qb.build());

        qb = new BooleanQuery.Builder();
        for (int i = 0; i < maxClauseCount; i++) {
            qb.add(
                new IndexOrDocValuesQuery(LongPoint.newRangeQuery("foo", 0, i), SortedNumericDocValuesField.newSlowRangeQuery("foo", 0, i)),
                BooleanClause.Occur.SHOULD
            );
        }
        // should not throw an exception, because it is below the limit
        searcher.rewrite(qb.build());

        reader.close();
        dir.close();
    }

    private static IndexSearcher newContextIndexSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            false,
            null,
            -1,
            1
        );
    }
}
