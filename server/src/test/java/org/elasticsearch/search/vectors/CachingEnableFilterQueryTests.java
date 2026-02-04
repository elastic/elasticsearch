/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CachingEnableFilterQueryTests extends ESTestCase {
    public void testEquals() {
        Query c1 = new CachingEnableFilterQuery(new TermQuery(new Term("foo", "bar")));
        Query c2 = new CachingEnableFilterQuery(new TermQuery(new Term("foo", "bar")));
        QueryUtils.checkEqual(c1, c2);

        c1 = new CachingEnableFilterQuery(new TermQuery(new Term("foo", "bar")));
        c2 = new CachingEnableFilterQuery(new TermQuery(new Term("foo", "baz")));
        QueryUtils.checkUnequal(c1, c2);

        c1 = new TermQuery(new Term("foo", "bar"));
        c2 = new CachingEnableFilterQuery(new TermQuery(new Term("foo", "baz")));
        QueryUtils.checkUnequal(c1, c2);
    }

    public void testTermQuery() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                if (i % 2 == 0) {
                    doc.add(newStringField("foo", "bar", Field.Store.YES));
                }
                iw.addDocument(doc);
            }
            iw.forceMerge(1);

            try (IndexReader reader = iw.getReader()) {
                assertTrue(reader.leaves().size() == 1 && reader.hasDeletions() == false);
                IndexSearcher searcher = newSearcher(reader);
                var termQuery = new TermQuery(new Term("foo", "bar"));
                Query query = new CachingEnableFilterQuery(termQuery);
                assertThat(searcher.rewrite(query), instanceOf(CachingEnableFilterQuery.class));
                assertEquals(5, searcher.count(query));

                var cachingPolicy = new UsageTrackingQueryCachingPolicy();
                searcher.setQueryCachingPolicy(cachingPolicy);
                var rewritten = searcher.rewrite(query);
                for (int i = 0; i < 5; i++) {
                    assertThat(searcher.search(rewritten, 10, Sort.INDEXORDER).totalHits.value(), equalTo(5L));
                }
                assertTrue(cachingPolicy.shouldCache(rewritten));

                for (int i = 0; i < 10; i++) {
                    assertThat(searcher.search(termQuery, 10, Sort.INDEXORDER).totalHits.value(), equalTo(5L));
                }
                assertFalse(cachingPolicy.shouldCache(termQuery));
            }
        }
    }

    public void testBooleanQuery() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                if (i % 2 == 0) {
                    doc.add(newStringField("f1", "bar", Field.Store.YES));
                }
                doc.add(newStringField("f2", "bar", Field.Store.YES));
                iw.addDocument(doc);
            }
            iw.forceMerge(1);

            try (IndexReader reader = iw.getReader()) {
                assertTrue(reader.leaves().size() == 1 && reader.hasDeletions() == false);
                IndexSearcher searcher = newSearcher(reader);
                var filter = new BooleanQuery.Builder().add(new TermQuery(new Term("f1", "bar")), FILTER)
                    .add(new TermQuery(new Term("f2", "bar")), FILTER)
                    .build();
                Query query = new CachingEnableFilterQuery(filter);
                assertThat(searcher.rewrite(query), instanceOf(BooleanQuery.class));
                assertEquals(5, searcher.count(query));

                filter = new BooleanQuery.Builder().add(new TermQuery(new Term("f1", "bar")), FILTER).build();
                query = new CachingEnableFilterQuery(filter);
                assertThat(searcher.rewrite(query), instanceOf(CachingEnableFilterQuery.class));
                assertEquals(5, searcher.count(query));

                var cachingPolicy = new UsageTrackingQueryCachingPolicy();
                searcher.setQueryCachingPolicy(cachingPolicy);
                var rewritten = searcher.rewrite(query);
                for (int i = 0; i < 5; i++) {
                    assertThat(searcher.search(rewritten, 10, Sort.INDEXORDER).totalHits.value(), equalTo(5L));
                }
                assertTrue(cachingPolicy.shouldCache(rewritten));
            }
        }
    }
}
