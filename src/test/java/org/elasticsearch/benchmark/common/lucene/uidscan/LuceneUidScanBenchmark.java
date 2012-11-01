/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.common.lucene.uidscan;

import jsr166y.ThreadLocalRandom;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.unit.SizeValue;

import java.io.File;
import java.util.concurrent.CountDownLatch;

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
            doc.add(new UidField("_uid", Long.toString(i), i));
            writer.addDocument(doc);
        }
        System.out.println("Done indexing, took " + watch.stop().lastTaskTime());

        final IndexReader reader = IndexReader.open(writer, true);

        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);
        Thread[] threads = new Thread[NUMBER_OF_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (long i = 0; i < SCAN_COUNT; i++) {
                            long id = startUid + (Math.abs(ThreadLocalRandom.current().nextInt()) % INDEX_COUNT);
                            DocsAndPositionsEnum uid = MultiFields.getTermPositionsEnum(reader,
                                    MultiFields.getLiveDocs(reader),
                                    "_uid",
                                    new BytesRef(Long.toString(id)));
                            uid.nextDoc();
                            uid.nextPosition();
                            if (uid.getPayload() == null) {
                                System.err.println("no payload...");
                                break;
                            }
                            BytesRef payload = uid.getPayload();
                            if (Numbers.bytesToLong(BytesRef.deepCopyOf(payload).bytes) != id) {
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
