/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class UUIDTests extends ESTestCase {

    static UUIDGenerator timeUUIDGen = new TimeBasedUUIDGenerator();
    static UUIDGenerator randomUUIDGen = new RandomBasedUUIDGenerator();

    public void testRandomUUID() {
        verifyUUIDSet(100000, randomUUIDGen);
    }

    public void testTimeUUID() {
        verifyUUIDSet(100000, timeUUIDGen);
    }

    public void testThreadedTimeUUID() {
        testUUIDThreaded(timeUUIDGen);
    }

    public void testThreadedRandomUUID() {
        testUUIDThreaded(randomUUIDGen);
    }

    Set<String> verifyUUIDSet(int count, UUIDGenerator uuidSource) {
        HashSet<String> uuidSet = new HashSet<>();
        for (int i = 0; i < count; ++i) {
            uuidSet.add(uuidSource.getBase64UUID());
        }
        assertEquals(count, uuidSet.size());
        return uuidSet;
    }

    class UUIDGenRunner implements Runnable {
        int count;
        public Set<String> uuidSet = null;
        UUIDGenerator uuidSource;

        UUIDGenRunner(int count, UUIDGenerator uuidSource) {
            this.count = count;
            this.uuidSource = uuidSource;
        }

        @Override
        public void run() {
            uuidSet = verifyUUIDSet(count, uuidSource);
        }
    }

    public void testUUIDThreaded(UUIDGenerator uuidSource) {
        HashSet<UUIDGenRunner> runners = new HashSet<>();
        HashSet<Thread> threads = new HashSet<>();
        int count = 20;
        int uuids = 10000;
        for (int i = 0; i < count; ++i) {
            UUIDGenRunner runner = new UUIDGenRunner(uuids, uuidSource);
            Thread t = new Thread(runner);
            threads.add(t);
            runners.add(runner);
        }
        for (Thread t : threads) {
            t.start();
        }
        boolean retry = false;
        do {
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException ie) {
                    retry = true;
                }
            }
        } while (retry);

        HashSet<String> globalSet = new HashSet<>();
        for (UUIDGenRunner runner : runners) {
            globalSet.addAll(runner.uuidSet);
        }
        assertEquals(count*uuids, globalSet.size());
    }

    public void testCompression() throws Exception {
        Logger logger = LogManager.getLogger(UUIDTests.class);
        // Low number so that the test runs quickly, but the results are more interesting with larger numbers
        // of indexed documents
        assertThat(testCompression(100000, 10000, 3, logger), Matchers.lessThan(14d)); // ~12 in practice
        assertThat(testCompression(100000, 1000, 3, logger), Matchers.lessThan(15d)); // ~13 in practice
        assertThat(testCompression(100000, 100, 3, logger), Matchers.lessThan(21d)); // ~20 in practice
    }

    private static double testCompression(int numDocs, int numDocsPerSecond, int numNodes, Logger logger) throws Exception {
        final double intervalBetweenDocs = 1000. / numDocsPerSecond; // milliseconds
        final byte[][] macAddresses = new byte[numNodes][];
        Random r = random();
        for (int i = 0; i < macAddresses.length; ++i) {
            macAddresses[i] = new byte[6];
            random().nextBytes(macAddresses[i]);
        }
        UUIDGenerator generator = new TimeBasedUUIDGenerator() {
            double currentTimeMillis = TestUtil.nextLong(random(), 0L, 10000000000L);

            @Override
            protected long currentTimeMillis() {
                currentTimeMillis += intervalBetweenDocs * 2 * r.nextDouble();
                return (long) currentTimeMillis;
            }

            @Override
            protected byte[] macAddress() {
                return RandomPicks.randomFrom(r, macAddresses);
            }
        };
        // Avoid randomization which will slow down things without improving
        // the quality of this test
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig config = new IndexWriterConfig()
                .setCodec(Codec.forName(Lucene.LATEST_CODEC))
                .setMergeScheduler(new SerialMergeScheduler()); // for reproducibility
        IndexWriter w = new IndexWriter(dir, config);
        Document doc = new Document();
        StringField id = new StringField("_id", "", Store.NO);
        doc.add(id);
        long start = System.nanoTime();
        for (int i = 0; i < numDocs; ++i) {
            id.setStringValue(generator.getBase64UUID());
            w.addDocument(doc);
        }
        w.forceMerge(1);
        long time = (System.nanoTime() - start) / 1000 / 1000;
        w.close();
        long size = 0;
        for (String file : dir.listAll()) {
            size += dir.fileLength(file);
        }
        dir.close();
        double bytesPerDoc = (double) size / numDocs;
        logger.info(numDocs + " docs indexed at " + numDocsPerSecond + " docs/s required " + new ByteSizeValue(size)
                + " bytes of disk space, or " + bytesPerDoc + " bytes per document. Took: " + new TimeValue(time) + ".");
        return bytesPerDoc;
    }
}
