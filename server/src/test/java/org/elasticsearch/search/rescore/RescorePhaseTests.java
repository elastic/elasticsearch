/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.FilteredSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.Collections;

public class RescorePhaseTests extends IndexShardTestCase {

    public void testRescorePhaseCancellation() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc)) {
                final int numDocs = scaledRandomIntBetween(100, 200);
                for (int i = 0; i < numDocs; ++i) {
                    Document doc = new Document();
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher s = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    new QueryCachingPolicy() {
                        @Override
                        public void onUse(Query query) {}

                        @Override
                        public boolean shouldCache(Query query) {
                            return false;
                        }
                    },
                    true
                );
                IndexShard shard = newShard(true);
                try (TestSearchContext context = new TestSearchContext(null, shard, s)) {
                    context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
                    SearchShardTask task = new SearchShardTask(123L, "", "", "", null, Collections.emptyMap());
                    context.setTask(task);
                    SearchContext wrapped = new FilteredSearchContext(context) {
                        @Override
                        public boolean lowLevelCancellation() {
                            return true;
                        }

                        @Override
                        public FetchDocValuesContext docValuesContext() {
                            return context.docValuesContext();
                        }

                        @Override
                        public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
                            return context.docValuesContext(docValuesContext);
                        }

                        @Override
                        public FetchFieldsContext fetchFieldsContext() {
                            return context.fetchFieldsContext();
                        }

                        @Override
                        public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
                            return context.fetchFieldsContext(fetchFieldsContext);
                        }
                    };
                    try (wrapped) {
                        Runnable cancellationChecks = RescorePhase.getCancellationChecks(wrapped);
                        assertNotNull(cancellationChecks);
                        TaskCancelHelper.cancel(task, "test cancellation");
                        assertTrue(wrapped.isCancelled());
                        expectThrows(TaskCancelledException.class, cancellationChecks::run);
                        QueryRescorer.QueryRescoreContext rescoreContext = new QueryRescorer.QueryRescoreContext(10);
                        rescoreContext.setQuery(new ParsedQuery(new MatchAllDocsQuery()));
                        rescoreContext.setCancellationChecker(cancellationChecks);
                        expectThrows(
                            TaskCancelledException.class,
                            () -> new QueryRescorer().rescore(
                                new TopDocs(
                                    new TotalHits(10, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                                    new ScoreDoc[] { new ScoreDoc(0, 1.0f) }
                                ),
                                context.searcher(),
                                rescoreContext
                            )
                        );
                    }
                }
                closeShards(shard);
            }
        }
    }
}
