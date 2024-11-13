/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Base64;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

public class UUIDTests extends ESTestCase {

    static final Base64.Decoder BASE_64_URL_DECODER = Base64.getUrlDecoder();
    static UUIDGenerator timeUUIDGen = new TimeBasedUUIDGenerator(
        UUIDs.DEFAULT_TIMESTAMP_SUPPLIER,
        UUIDs.DEFAULT_SEQUENCE_ID_SUPPLIER,
        UUIDs.DEFAULT_MAC_ADDRESS_SUPPLIER
    );
    static UUIDGenerator randomUUIDGen = new RandomBasedUUIDGenerator();
    static UUIDGenerator kOrderedUUIDGen = new TimeBasedKOrderedUUIDGenerator(
        UUIDs.DEFAULT_TIMESTAMP_SUPPLIER,
        UUIDs.DEFAULT_SEQUENCE_ID_SUPPLIER,
        UUIDs.DEFAULT_MAC_ADDRESS_SUPPLIER
    );

    public void testRandomUUID() {
        verifyUUIDSet(100000, randomUUIDGen).forEach(this::verifyUUIDIsUrlSafe);
    }

    public void testTimeUUID() {
        verifyUUIDSet(100000, timeUUIDGen).forEach(this::verifyUUIDIsUrlSafe);
    }

    public void testKOrderedUUID() {
        verifyUUIDSet(100000, kOrderedUUIDGen).forEach(this::verifyUUIDIsUrlSafe);
    }

    public void testThreadedRandomUUID() {
        testUUIDThreaded(randomUUIDGen);
    }

    public void testThreadedTimeUUID() {
        testUUIDThreaded(timeUUIDGen);
    }

    public void testThreadedKOrderedUUID() {
        testUUIDThreaded(kOrderedUUIDGen);
    }

    public void testCompression() throws Exception {
        Logger logger = LogManager.getLogger(UUIDTests.class);

        assertThat(testCompression(timeUUIDGen, 100000, 10000, 3, logger), Matchers.lessThan(14d));
        assertThat(testCompression(timeUUIDGen, 100000, 1000, 3, logger), Matchers.lessThan(15d));
        assertThat(testCompression(timeUUIDGen, 100000, 100, 3, logger), Matchers.lessThan(21d));

        assertThat(testCompression(kOrderedUUIDGen, 100000, 10000, 3, logger), Matchers.lessThan(13d));
        assertThat(testCompression(kOrderedUUIDGen, 100000, 1000, 3, logger), Matchers.lessThan(14d));
        assertThat(testCompression(kOrderedUUIDGen, 100000, 100, 3, logger), Matchers.lessThan(19d));
    }

    public void testComparativeCompression() throws Exception {
        Logger logger = LogManager.getLogger(UUIDTests.class);

        int numDocs = 100000;
        int docsPerSecond = 1000;
        int nodes = 3;

        double randomCompression = testCompression(randomUUIDGen, numDocs, docsPerSecond, nodes, logger);
        double baseCompression = testCompression(timeUUIDGen, numDocs, docsPerSecond, nodes, logger);
        double kOrderedCompression = testCompression(kOrderedUUIDGen, numDocs, docsPerSecond, nodes, logger);

        assertThat(kOrderedCompression, Matchers.lessThanOrEqualTo(baseCompression));
        assertThat(kOrderedCompression, Matchers.lessThanOrEqualTo(randomCompression));
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
        assertEquals(count * uuids, globalSet.size());
        globalSet.forEach(this::verifyUUIDIsUrlSafe);
    }

    private static double testCompression(final UUIDGenerator generator, int numDocs, int numDocsPerSecond, int numNodes, Logger logger)
        throws Exception {
        final double intervalBetweenDocs = 1000. / numDocsPerSecond;
        final byte[][] macAddresses = new byte[numNodes][];
        Random r = random();
        for (int i = 0; i < macAddresses.length; ++i) {
            macAddresses[i] = new byte[6];
            random().nextBytes(macAddresses[i]);
        }

        UUIDGenerator uuidSource = generator;
        if (generator instanceof TimeBasedUUIDGenerator) {
            if (generator instanceof TimeBasedKOrderedUUIDGenerator) {
                uuidSource = new TimeBasedKOrderedUUIDGenerator(new Supplier<>() {
                    double currentTimeMillis = TestUtil.nextLong(random(), 0L, 10000000000L);

                    @Override
                    public Long get() {
                        currentTimeMillis += intervalBetweenDocs * 2 * r.nextDouble();
                        return (long) currentTimeMillis;
                    }
                }, () -> 0, () -> RandomPicks.randomFrom(r, macAddresses));
            } else {
                uuidSource = new TimeBasedUUIDGenerator(new Supplier<>() {
                    double currentTimeMillis = TestUtil.nextLong(random(), 0L, 10000000000L);

                    @Override
                    public Long get() {
                        currentTimeMillis += intervalBetweenDocs * 2 * r.nextDouble();
                        return (long) currentTimeMillis;
                    }
                }, () -> 0, () -> RandomPicks.randomFrom(r, macAddresses));
            }
        }

        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig config = new IndexWriterConfig().setCodec(Codec.forName(Lucene.LATEST_CODEC))
            .setMergeScheduler(new SerialMergeScheduler());

        IndexWriter w = new IndexWriter(dir, config);
        Document doc = new Document();
        StringField id = new StringField("_id", "", Store.NO);
        doc.add(id);
        long start = System.nanoTime();
        for (int i = 0; i < numDocs; ++i) {
            id.setStringValue(uuidSource.getBase64UUID());
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
        logger.info(
            "{} - {} docs indexed at {} docs/s required {} bytes of disk space, or {} bytes per document. Took: {}.",
            uuidSource.getClass().getSimpleName(),
            numDocs,
            numDocsPerSecond,
            ByteSizeValue.ofBytes(size),
            bytesPerDoc,
            new TimeValue(time)
        );
        return bytesPerDoc;
    }

    public void testStringLength() {
        assertEquals(UUIDs.RANDOM_BASED_UUID_STRING_LENGTH, getUnpaddedBase64StringLength(RandomBasedUUIDGenerator.SIZE_IN_BYTES));
        assertEquals(UUIDs.TIME_BASED_UUID_STRING_LENGTH, getUnpaddedBase64StringLength(TimeBasedUUIDGenerator.SIZE_IN_BYTES));
        assertEquals(UUIDs.TIME_BASED_UUID_STRING_LENGTH, getUnpaddedBase64StringLength(TimeBasedKOrderedUUIDGenerator.SIZE_IN_BYTES));

        assertEquals(UUIDs.RANDOM_BASED_UUID_STRING_LENGTH, randomUUIDGen.getBase64UUID().length());
        assertEquals(UUIDs.TIME_BASED_UUID_STRING_LENGTH, timeUUIDGen.getBase64UUID().length());
        assertEquals(UUIDs.TIME_BASED_UUID_STRING_LENGTH, kOrderedUUIDGen.getBase64UUID().length());
    }

    private static int getUnpaddedBase64StringLength(int sizeInBytes) {
        return (int) Math.ceil(sizeInBytes * 4.0 / 3.0);
    }

    private void verifyUUIDIsUrlSafe(final String uuid) {
        assertFalse("UUID should not contain padding characters: " + uuid, uuid.contains("="));
        try {
            BASE_64_URL_DECODER.decode(uuid);
        } catch (IllegalArgumentException e) {
            throw new AssertionError("UUID is not a valid Base64 URL-safe encoded string: " + uuid);
        }
    }
}
