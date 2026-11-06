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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.equalTo;

public class SearcherExecutionChargeTests extends ESTestCase {

    public void testASearcherChargeLastsUntilTheSearcherCloses() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
                final ContextIndexSearcher searcher = BreakerWeightTestUtils.newContextIndexSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                searcher.chargeSearcher(100, "test");
                searcher.chargeSearcher(28, "test");
                assertThat(breaker.getUsed(), equalTo(128L));
                assertThat(searcher.searcherExecutionBytes(), equalTo(128L));
                searcher.close();
                assertThat(breaker.getUsed(), equalTo(0L));
                assertThat(searcher.searcherExecutionBytes(), equalTo(0L));
            }
        }
    }

    public void testATrippedSearcherChargeRecordsNothing() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(150L);
                final ContextIndexSearcher searcher = BreakerWeightTestUtils.newContextIndexSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                searcher.chargeSearcher(100, "test");
                expectThrows(CircuitBreakingException.class, () -> searcher.chargeSearcher(100, "test"));
                assertThat(searcher.searcherExecutionBytes(), equalTo(100L));
                searcher.close();
                assertThat(breaker.getUsed(), equalTo(0L));
            }
        }
    }

    public void testReplacingTheBreakerReleasesTheSearcherCharges() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final TrackingCircuitBreaker first = new TrackingCircuitBreaker(-1L);
                final TrackingCircuitBreaker second = new TrackingCircuitBreaker(-1L);
                final ContextIndexSearcher searcher = BreakerWeightTestUtils.newContextIndexSearcher(reader);
                searcher.setCircuitBreaker(first);
                searcher.chargeSearcher(64, "test");
                searcher.setCircuitBreaker(second);
                assertThat(first.getUsed(), equalTo(0L));
                searcher.chargeSearcher(32, "test");
                searcher.close();
                assertThat(second.getUsed(), equalTo(0L));
            }
        }
    }

    public void testWithoutABreakerNothingIsCharged() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ContextIndexSearcher searcher = BreakerWeightTestUtils.newContextIndexSearcher(reader);
                searcher.chargeSearcher(1_000, "test");
                assertThat(searcher.searcherExecutionBytes(), equalTo(0L));
                searcher.close();
            }
        }
    }

    public void testConcurrentChargesAreAllReleased() throws Exception {
        try (Directory dir = newDirectory()) {
            index(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(-1L);
                final ContextIndexSearcher searcher = BreakerWeightTestUtils.newContextIndexSearcher(reader);
                searcher.setCircuitBreaker(breaker);
                final int threads = between(2, 8);
                final int perThread = between(10, 200);
                final ExecutorService executor = Executors.newFixedThreadPool(threads);
                try {
                    final List<Future<?>> done = new ArrayList<>();
                    for (int t = 0; t < threads; t++) {
                        done.add(executor.submit(() -> {
                            for (int i = 0; i < perThread; i++) {
                                searcher.chargeSearcher(8, "test");
                            }
                        }));
                    }
                    for (Future<?> future : done) {
                        future.get();
                    }
                } finally {
                    terminate(executor);
                }
                assertThat(breaker.getUsed(), equalTo(8L * threads * perThread));
                searcher.close();
                assertThat(breaker.getUsed(), equalTo(0L));
            }
        }
    }

    private static void index(Directory dir) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            final Document doc = new Document();
            doc.add(new StringField("f", "v", StringField.Store.NO));
            writer.addDocument(doc);
        }
    }
}
