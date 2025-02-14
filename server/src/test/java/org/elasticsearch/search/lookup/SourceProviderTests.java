/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SourceProviderTests extends ESTestCase {

    public void testStoredFieldsSourceProvider() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.YES));
            doc.add(new StoredField("_source", new BytesRef("{\"field\": \"value\"}")));
            iw.addDocument(doc);

            try (IndexReader reader = iw.getReader()) {
                LeafReaderContext readerContext = reader.leaves().get(0);

                SourceProvider sourceProvider = SourceProvider.fromStoredFields();
                Source source = sourceProvider.getSource(readerContext, 0);

                assertNotNull(source.internalSourceRef());

                // Source should be preserved if we pass in the same reader and document
                Source s2 = sourceProvider.getSource(readerContext, 0);
                assertSame(s2, source);
            }
        }
    }

    public void testConcurrentStoredFieldsSourceProvider() throws IOException {
        int numDocs = 350;
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc)) {

            Document doc = new Document();
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                doc.add(new StoredField("_source", new BytesRef("{ \"id\" : " + i + "}")));
                iw.addDocument(doc);
                if (random().nextInt(35) == 7) {
                    iw.commit();
                }
            }
            iw.commit();

            IndexReader reader = iw.getReader();
            IndexSearcher searcher = new IndexSearcher(reader, executorService);

            int numIterations = 20;
            for (int i = 0; i < numIterations; i++) {
                searcher.search(new MatchAllDocsQuery(), assertingCollectorManager());
            }

            reader.close();
        }
        executorService.shutdown();
    }

    private static class SourceAssertingCollector implements Collector {

        final SourceProvider sourceProvider;

        private SourceAssertingCollector(SourceProvider sourceProvider) {
            this.sourceProvider = sourceProvider;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {

                }

                @Override
                public void collect(int doc) throws IOException {
                    Source source = sourceProvider.getSource(context, doc);
                    assertEquals(doc + context.docBase, source.source().get("id"));
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }
    }

    private static CollectorManager<SourceAssertingCollector, ?> assertingCollectorManager() {
        SourceProvider sourceProvider = SourceProvider.fromStoredFields();
        return new CollectorManager<>() {
            @Override
            public SourceAssertingCollector newCollector() {
                return new SourceAssertingCollector(sourceProvider);
            }

            @Override
            public Object reduce(Collection<SourceAssertingCollector> collectors) {
                return 0;
            }
        };
    }
}
