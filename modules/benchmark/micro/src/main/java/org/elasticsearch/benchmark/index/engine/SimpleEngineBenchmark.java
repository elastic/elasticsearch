/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LoadFirstFieldSelector;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.cache.memory.ByteBufferCache;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.bloom.none.NoneBloomCache;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.robin.RobinEngine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.memory.ByteBufferStore;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.lucene.DocumentBuilder.*;
import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (Shay Banon)
 */
public class SimpleEngineBenchmark {

    private final Store store;

    private final Engine engine;


    private final AtomicInteger idGenerator = new AtomicInteger();

    private String[] contentItems = new String[]{"test1", "test2", "test3"};

    private static byte[] TRANSLOG_PAYLOAD = new byte[12];

    private volatile int lastRefreshedId = 0;


    private boolean create = false;

    private int searcherIterations = 10;

    private Thread[] searcherThreads = new Thread[1];

    private int writerIterations = 10;

    private Thread[] writerThreads = new Thread[1];

    private TimeValue refreshSchedule = new TimeValue(1, TimeUnit.SECONDS);

    private TimeValue flushSchedule = new TimeValue(1, TimeUnit.MINUTES);


    private CountDownLatch latch;
    private CyclicBarrier barrier1;
    private CyclicBarrier barrier2;


    // scheduled thread pool for both refresh and flush operations
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

    public SimpleEngineBenchmark(Store store, Engine engine) {
        this.store = store;
        this.engine = engine;
    }

    public SimpleEngineBenchmark numberOfContentItems(int numberOfContentItems) {
        contentItems = new String[numberOfContentItems];
        for (int i = 0; i < contentItems.length; i++) {
            contentItems[i] = "content" + i;
        }
        return this;
    }

    public SimpleEngineBenchmark searcherThreads(int numberOfSearcherThreads) {
        searcherThreads = new Thread[numberOfSearcherThreads];
        return this;
    }

    public SimpleEngineBenchmark searcherIterations(int searcherIterations) {
        this.searcherIterations = searcherIterations;
        return this;
    }

    public SimpleEngineBenchmark writerThreads(int numberOfWriterThreads) {
        writerThreads = new Thread[numberOfWriterThreads];
        return this;
    }

    public SimpleEngineBenchmark writerIterations(int writerIterations) {
        this.writerIterations = writerIterations;
        return this;
    }

    public SimpleEngineBenchmark refreshSchedule(TimeValue refreshSchedule) {
        this.refreshSchedule = refreshSchedule;
        return this;
    }

    public SimpleEngineBenchmark flushSchedule(TimeValue flushSchedule) {
        this.flushSchedule = flushSchedule;
        return this;
    }

    public SimpleEngineBenchmark create(boolean create) {
        this.create = create;
        return this;
    }

    public SimpleEngineBenchmark build() {
        for (int i = 0; i < searcherThreads.length; i++) {
            searcherThreads[i] = new Thread(new SearcherThread(), "Searcher[" + i + "]");
        }
        for (int i = 0; i < writerThreads.length; i++) {
            writerThreads[i] = new Thread(new WriterThread(), "Writer[" + i + "]");
        }

        latch = new CountDownLatch(searcherThreads.length + writerThreads.length);
        barrier1 = new CyclicBarrier(searcherThreads.length + writerThreads.length + 1);
        barrier2 = new CyclicBarrier(searcherThreads.length + writerThreads.length + 1);

        // warmup by indexing all content items
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (String contentItem : contentItems) {
            int id = idGenerator.incrementAndGet();
            String sId = Integer.toString(id);
            Document doc = doc().add(field("_id", sId))
                    .add(field("content", contentItem)).build();
            ParsedDocument pDoc = new ParsedDocument(sId, sId, "type", null, doc, Lucene.STANDARD_ANALYZER, TRANSLOG_PAYLOAD, false);
            if (create) {
                engine.create(new Engine.Create(null, new Term("_id", sId), pDoc));
            } else {
                engine.index(new Engine.Index(null, new Term("_id", sId), pDoc));
            }
        }
        engine.refresh(new Engine.Refresh(true));
        stopWatch.stop();
        System.out.println("Warmup of [" + contentItems.length + "] content items, took " + stopWatch.totalTime());

        return this;
    }

    public void run() throws Exception {
        for (Thread t : searcherThreads) {
            t.start();
        }
        for (Thread t : writerThreads) {
            t.start();
        }
        barrier1.await();

        Refresher refresher = new Refresher();
        scheduledExecutorService.scheduleWithFixedDelay(refresher, refreshSchedule.millis(), refreshSchedule.millis(), TimeUnit.MILLISECONDS);
        Flusher flusher = new Flusher();
        scheduledExecutorService.scheduleWithFixedDelay(flusher, flushSchedule.millis(), flushSchedule.millis(), TimeUnit.MILLISECONDS);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        barrier2.await();

        latch.await();
        stopWatch.stop();

        System.out.println("Summary");
        System.out.println("   -- Readers [" + searcherThreads.length + "] with [" + searcherIterations + "] iterations");
        System.out.println("   -- Writers [" + writerThreads.length + "] with [" + writerIterations + "] iterations");
        System.out.println("   -- Took: " + stopWatch.totalTime());
        System.out.println("   -- Refresh [" + refresher.id + "] took: " + refresher.stopWatch.totalTime());
        System.out.println("   -- Flush [" + flusher.id + "] took: " + flusher.stopWatch.totalTime());
        System.out.println("   -- Store size " + store.estimateSize());

        scheduledExecutorService.shutdown();

        engine.refresh(new Engine.Refresh(true));
        stopWatch = new StopWatch();
        stopWatch.start();
        Engine.Searcher searcher = engine.searcher();
        TopDocs topDocs = searcher.searcher().search(new MatchAllDocsQuery(), idGenerator.get() + 1);
        stopWatch.stop();
        System.out.println("   -- Indexed [" + idGenerator.get() + "] docs, found [" + topDocs.totalHits + "] hits, took " + stopWatch.totalTime());
        searcher.release();
    }

    private String content(long number) {
        return contentItems[((int) (number % contentItems.length))];
    }

    private class Flusher implements Runnable {
        StopWatch stopWatch = new StopWatch();
        private int id;

        @Override public void run() {
            stopWatch.start("" + ++id);
            engine.flush(new Engine.Flush());
            stopWatch.stop();
        }
    }

    private class Refresher implements Runnable {
        StopWatch stopWatch = new StopWatch();
        private int id;

        @Override public synchronized void run() {
            stopWatch.start("" + ++id);
            int lastId = idGenerator.get();
            engine.refresh(new Engine.Refresh(true));
            lastRefreshedId = lastId;
            stopWatch.stop();
        }
    }

    private class SearcherThread implements Runnable {
        @Override public void run() {
            try {
                barrier1.await();
                barrier2.await();
                for (int i = 0; i < searcherIterations; i++) {
                    Engine.Searcher searcher = engine.searcher();
                    TopDocs topDocs = searcher.searcher().search(new TermQuery(new Term("content", content(i))), 10);
                    // read one
                    searcher.searcher().doc(topDocs.scoreDocs[0].doc, new LoadFirstFieldSelector());
                    searcher.release();
                }
            } catch (Exception e) {
                System.out.println("Searcher thread failed");
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    private class WriterThread implements Runnable {
        @Override public void run() {
            try {
                barrier1.await();
                barrier2.await();
                for (int i = 0; i < writerIterations; i++) {
                    int id = idGenerator.incrementAndGet();
                    String sId = Integer.toString(id);
                    Document doc = doc().add(field("_id", sId))
                            .add(field("content", content(id))).build();
                    ParsedDocument pDoc = new ParsedDocument(sId, sId, "type", null, doc, Lucene.STANDARD_ANALYZER, TRANSLOG_PAYLOAD, false);
                    if (create) {
                        engine.create(new Engine.Create(null, new Term("_id", sId), pDoc));
                    } else {
                        engine.index(new Engine.Index(null, new Term("_id", sId), pDoc));
                    }
                }
            } catch (Exception e) {
                System.out.println("Writer thread failed");
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ShardId shardId = new ShardId(new Index("index"), 1);
        Settings settings = EMPTY_SETTINGS;

//        Store store = new RamStore(shardId, settings);
        Store store = new ByteBufferStore(shardId, settings, null, new ByteBufferCache(settings));
//        Store store = new NioFsStore(shardId, settings);

        store.deleteContent();

        ThreadPool threadPool = new ThreadPool();
        SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastDeletionPolicy(shardId, settings));
        Engine engine = new RobinEngine(shardId, settings, new ThreadPool(), new IndexSettingsService(shardId.index(), settings), store, deletionPolicy, new FsTranslog(shardId, EMPTY_SETTINGS, new File("work/fs-translog"), false), new LogByteSizeMergePolicyProvider(store, new IndexSettingsService(shardId.index(), EMPTY_SETTINGS)),
                new ConcurrentMergeSchedulerProvider(shardId, settings), new AnalysisService(shardId.index()), new SimilarityService(shardId.index()), new NoneBloomCache(shardId.index()));
        engine.start();

        SimpleEngineBenchmark benchmark = new SimpleEngineBenchmark(store, engine)
                .numberOfContentItems(1000)
                .searcherThreads(50).searcherIterations(10000)
                .writerThreads(10).writerIterations(10000)
                .refreshSchedule(new TimeValue(1, TimeUnit.SECONDS))
                .flushSchedule(new TimeValue(1, TimeUnit.MINUTES))
                .create(false)
                .build();

        benchmark.run();

        engine.close();
        store.close();
        threadPool.shutdown();
    }
}
