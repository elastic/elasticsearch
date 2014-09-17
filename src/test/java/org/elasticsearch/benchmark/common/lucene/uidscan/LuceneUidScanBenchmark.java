/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.benchmark.common.lucene.uidscan;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.SizeValue;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class LuceneUidScanBenchmark {

    public static void main(String[] args) throws Exception {

        FSDirectory dir = FSDirectory.open(new File("work/test"));
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        final int NUMBER_OF_THREADS = 2;
        final long INDEX_COUNT = SizeValue.parseSizeValue("1m").singles();
        final long SCAN_COUNT = SizeValue.parseSizeValue("100k").singles();
        final long startUid = 1000000;

        long LIMIT = startUid + INDEX_COUNT;
        StopWatch watch = new StopWatch().start();
        System.out.println("Indexing " + INDEX_COUNT + " docs...");
        for (long i = startUid; i < LIMIT; i++) {
            Document doc = new Document();
            doc.add(new StringField("_uid", Long.toString(i), Store.NO));
            doc.add(new NumericDocValuesField("_version", i));
            writer.addDocument(doc);
        }
        System.out.println("Done indexing, took " + watch.stop().lastTaskTime());

        final IndexReader reader = DirectoryReader.open(writer, true);

        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);
        Thread[] threads = new Thread[NUMBER_OF_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (long i = 0; i < SCAN_COUNT; i++) {
                            long id = startUid + (Math.abs(ThreadLocalRandom.current().nextInt()) % INDEX_COUNT);
                            final long version = Versions.loadVersion(reader, new Term("_uid", Long.toString(id)));
                            if (version != id) {
                                System.err.println("wrong id...");
                                break;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        watch = new StopWatch().start();
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        latch.await();
        watch.stop();
        System.out.println("Scanned in " + watch.totalTime() + " TP Seconds " + ((SCAN_COUNT * NUMBER_OF_THREADS) / watch.totalTime().secondsFrac()));
    }
}
