/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.versioning;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFutureThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SimpleVersioningIT extends ESIntegTestCase {
    public void testExternalVersioningInitialDelete() throws Exception {
        createIndex("test");
        ensureGreen();

        // Note - external version doesn't throw version conflicts on deletes of non existent records.
        // This is different from internal versioning

        DeleteResponse deleteResponse = client().prepareDelete("test", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).get();
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteResponse.getResult());

        // this should conflict with the delete command transaction which told us that the object was deleted at version 17.
        assertFutureThrows(
            prepareIndex("test").setId("1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class
        );

        DocWriteResponse indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(18)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(18L));
    }

    public void testExternalGTE() throws Exception {
        createIndex("test");

        DocWriteResponse indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(12)
            .setVersionType(VersionType.EXTERNAL_GTE)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(12L));

        indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_2")
            .setVersion(12)
            .setVersionType(VersionType.EXTERNAL_GTE)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(12L));

        indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_2")
            .setVersion(14)
            .setVersionType(VersionType.EXTERNAL_GTE)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(14L));

        RequestBuilder<?, ?> builder1 = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(13)
            .setVersionType(VersionType.EXTERNAL_GTE);
        expectThrows(VersionConflictEngineException.class, builder1);

        client().admin().indices().prepareRefresh().get();
        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "1").get().getVersion(), equalTo(14L));
        }

        // deleting with a lower version fails.
        RequestBuilder<?, ?> builder = client().prepareDelete("test", "1").setVersion(2).setVersionType(VersionType.EXTERNAL_GTE);
        expectThrows(VersionConflictEngineException.class, builder);

        // Delete with a higher or equal version deletes all versions up to the given one.
        long v = randomIntBetween(14, 17);
        DeleteResponse deleteResponse = client().prepareDelete("test", "1").setVersion(v).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getVersion(), equalTo(v));

        // Deleting with a lower version keeps on failing after a delete.
        assertFutureThrows(
            client().prepareDelete("test", "1").setVersion(2).setVersionType(VersionType.EXTERNAL_GTE).execute(),
            VersionConflictEngineException.class
        );

        // But delete with a higher version is OK.
        deleteResponse = client().prepareDelete("test", "1").setVersion(18).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteResponse.getResult());
        assertThat(deleteResponse.getVersion(), equalTo(18L));
    }

    public void testExternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        DocWriteResponse indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(12)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(12L));

        indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(14)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(14L));

        assertFutureThrows(
            prepareIndex("test").setId("1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class
        );

        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "1").get().getVersion(), equalTo(14L));
        }

        // deleting with a lower version fails.
        assertFutureThrows(
            client().prepareDelete("test", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class
        );

        // Delete with a higher version deletes all versions up to the given one.
        DeleteResponse deleteResponse = client().prepareDelete("test", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getVersion(), equalTo(17L));

        // Deleting with a lower version keeps on failing after a delete.
        assertFutureThrows(
            client().prepareDelete("test", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class
        );

        // But delete with a higher version is OK.
        deleteResponse = client().prepareDelete("test", "1").setVersion(18).setVersionType(VersionType.EXTERNAL).get();
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteResponse.getResult());
        assertThat(deleteResponse.getVersion(), equalTo(18L));

        // TODO: This behavior breaks rest api returning http status 201
        // good news is that it this is only the case until deletes GC kicks in.
        indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(19)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(19L));

        deleteResponse = client().prepareDelete("test", "1").setVersion(20).setVersionType(VersionType.EXTERNAL).get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getVersion(), equalTo(20L));

        // Make sure that the next delete will be GC. Note we do it on the index settings so it will be cleaned up
        updateIndexSettings(Settings.builder().put("index.gc_deletes", -1), "test");
        Thread.sleep(300); // gc works based on estimated sampled time. Give it a chance...

        // And now we have previous version return -1
        indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(20)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertThat(indexResponse.getVersion(), equalTo(20L));
    }

    public void testRequireUnitsOnUpdateSettings() throws Exception {
        createIndex("test");
        ensureGreen();
        HashMap<String, Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes", "42");
        try {
            client().admin().indices().prepareUpdateSettings("test").setSettings(newSettings).get();
            fail("did not hit expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
            assertTrue(
                iae.getMessage()
                    .contains("failed to parse setting [index.gc_deletes] with value [42] as a time value: unit is missing or unrecognized")
            );
        }
    }

    public void testCompareAndSetInitialDelete() throws Exception {
        createIndex("test");
        ensureGreen();

        assertFutureThrows(
            client().prepareDelete("test", "1").setIfSeqNo(17).setIfPrimaryTerm(10).execute(),
            VersionConflictEngineException.class
        );

        DocWriteResponse indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_1").setCreate(true).get();
        assertThat(indexResponse.getVersion(), equalTo(1L));
    }

    public void testCompareAndSet() {
        createIndex("test");
        ensureGreen();

        DocWriteResponse indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_1").get();
        assertThat(indexResponse.getSeqNo(), equalTo(0L));
        assertThat(indexResponse.getPrimaryTerm(), equalTo(1L));

        indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_2").setIfSeqNo(0L).setIfPrimaryTerm(1).get();
        assertThat(indexResponse.getSeqNo(), equalTo(1L));
        assertThat(indexResponse.getPrimaryTerm(), equalTo(1L));

        assertFutureThrows(
            prepareIndex("test").setId("1").setSource("field1", "value1_1").setIfSeqNo(10).setIfPrimaryTerm(1).execute(),
            VersionConflictEngineException.class
        );

        assertFutureThrows(
            prepareIndex("test").setId("1").setSource("field1", "value1_1").setIfSeqNo(10).setIfPrimaryTerm(2).execute(),
            VersionConflictEngineException.class
        );

        assertFutureThrows(
            prepareIndex("test").setId("1").setSource("field1", "value1_1").setIfSeqNo(1).setIfPrimaryTerm(2).execute(),
            VersionConflictEngineException.class
        );

        RequestBuilder<?, ?> builder6 = client().prepareDelete("test", "1").setIfSeqNo(10).setIfPrimaryTerm(1);
        expectThrows(VersionConflictEngineException.class, builder6);
        RequestBuilder<?, ?> builder5 = client().prepareDelete("test", "1").setIfSeqNo(10).setIfPrimaryTerm(2);
        expectThrows(VersionConflictEngineException.class, builder5);
        RequestBuilder<?, ?> builder4 = client().prepareDelete("test", "1").setIfSeqNo(1).setIfPrimaryTerm(2);
        expectThrows(VersionConflictEngineException.class, builder4);

        client().admin().indices().prepareRefresh().get();
        for (int i = 0; i < 10; i++) {
            final GetResponse response = client().prepareGet("test", "1").get();
            assertThat(response.getSeqNo(), equalTo(1L));
            assertThat(response.getPrimaryTerm(), equalTo(1L));
        }

        // search with versioning
        for (int i = 0; i < 10; i++) {
            // TODO: ADD SEQ NO!
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()).setVersion(true),
                response -> assertThat(response.getHits().getAt(0).getVersion(), equalTo(2L))
            );
        }

        // search without versioning
        for (int i = 0; i < 10; i++) {
            assertResponse(
                prepareSearch().setQuery(matchAllQuery()),
                response -> assertThat(response.getHits().getAt(0).getVersion(), equalTo(Versions.NOT_FOUND))
            );
        }

        DeleteResponse deleteResponse = client().prepareDelete("test", "1").setIfSeqNo(1).setIfPrimaryTerm(1).get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        assertThat(deleteResponse.getSeqNo(), equalTo(2L));
        assertThat(deleteResponse.getPrimaryTerm(), equalTo(1L));

        RequestBuilder<?, ?> builder3 = client().prepareDelete("test", "1").setIfSeqNo(1).setIfPrimaryTerm(1);
        expectThrows(VersionConflictEngineException.class, builder3);
        RequestBuilder<?, ?> builder2 = client().prepareDelete("test", "1").setIfSeqNo(3).setIfPrimaryTerm(12);
        expectThrows(VersionConflictEngineException.class, builder2);
        RequestBuilder<?, ?> builder1 = client().prepareDelete("test", "1").setIfSeqNo(1).setIfPrimaryTerm(2);
        expectThrows(VersionConflictEngineException.class, builder1);

        // the doc is deleted. Even when we hit the deleted seqNo, a conditional delete should fail.
        RequestBuilder<?, ?> builder = client().prepareDelete("test", "1").setIfSeqNo(2).setIfPrimaryTerm(1);
        expectThrows(VersionConflictEngineException.class, builder);
    }

    public void testSimpleVersioningWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        DocWriteResponse indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_1").get();
        assertThat(indexResponse.getSeqNo(), equalTo(0L));

        client().admin().indices().prepareFlush().get();
        indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_2").setIfSeqNo(0).setIfPrimaryTerm(1).get();
        assertThat(indexResponse.getSeqNo(), equalTo(1L));

        client().admin().indices().prepareFlush().get();
        RequestBuilder<?, ?> builder2 = prepareIndex("test").setId("1").setSource("field1", "value1_1").setIfSeqNo(0).setIfPrimaryTerm(1);
        expectThrows(VersionConflictEngineException.class, builder2);

        RequestBuilder<?, ?> builder1 = prepareIndex("test").setId("1").setCreate(true).setSource("field1", "value1_1");
        expectThrows(VersionConflictEngineException.class, builder1);

        RequestBuilder<?, ?> builder = client().prepareDelete("test", "1").setIfSeqNo(0).setIfPrimaryTerm(1);
        expectThrows(VersionConflictEngineException.class, builder);

        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "1").get().getVersion(), equalTo(2L));
        }

        client().admin().indices().prepareRefresh().get();

        for (int i = 0; i < 10; i++) {
            assertResponse(prepareSearch().setQuery(matchAllQuery()).setVersion(true).seqNoAndPrimaryTerm(true), response -> {
                assertHitCount(response, 1);
                assertThat(response.getHits().getAt(0).getVersion(), equalTo(2L));
                assertThat(response.getHits().getAt(0).getSeqNo(), equalTo(1L));
            });
        }
    }

    public void testVersioningWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk().add(prepareIndex("test").setId("1").setSource("field1", "value1_1")).get();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertThat(indexResponse.getVersion(), equalTo(1L));
    }

    // Poached from Lucene's TestIDVersionPostingsFormat:

    private interface IDSource {
        String next();
    }

    private IDSource getRandomIDs() {
        final Random random = random();
        return switch (random.nextInt(6)) {
            case 0 -> {
                // random simple
                logger.info("--> use random simple ids");
                yield new IDSource() {
                    @Override
                    public String next() {
                        return TestUtil.randomSimpleString(random, 1, 10);
                    }
                };
            }
            case 1 -> {
                // random realistic unicode
                logger.info("--> use random realistic unicode ids");
                yield new IDSource() {
                    @Override
                    public String next() {
                        return TestUtil.randomRealisticUnicodeString(random, 1, 20);
                    }
                };
            }
            case 2 -> {
                // sequential
                logger.info("--> use sequential ids");
                yield new IDSource() {
                    int upto;

                    @Override
                    public String next() {
                        return Integer.toString(upto++);
                    }
                };
            }
            case 3 -> {
                // zero-pad sequential
                logger.info("--> use zero-padded sequential ids");
                yield new IDSource() {
                    final String zeroPad = Strings.format("%0" + TestUtil.nextInt(random, 4, 20) + "d", 0);
                    int upto;

                    @Override
                    public String next() {
                        String s = Integer.toString(upto++);
                        return zeroPad.substring(zeroPad.length() - s.length()) + s;
                    }
                };
            }
            case 4 -> {
                // random long
                logger.info("--> use random long ids");
                yield new IDSource() {
                    final int radix = TestUtil.nextInt(random, Character.MIN_RADIX, Character.MAX_RADIX);

                    @Override
                    public String next() {
                        return Long.toString(random.nextLong() & 0x3ffffffffffffffL, radix);
                    }
                };
            }
            case 5 -> {
                // zero-pad random long
                logger.info("--> use zero-padded random long ids");
                yield new IDSource() {
                    final int radix = TestUtil.nextInt(random, Character.MIN_RADIX, Character.MAX_RADIX);

                    @Override
                    public String next() {
                        return Long.toString(random.nextLong() & 0x3ffffffffffffffL, radix);
                    }
                };
            }
            default -> throw new AssertionError();
        };
    }

    private static class IDAndVersion {
        public String id;
        public long version;
        public boolean delete;
        public int threadID = -1;
        public long indexStartTime;
        public long indexFinishTime;
        public boolean versionConflict;
        public boolean alreadyExists;
        public ActionResponse response;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("id=");
            sb.append(id);
            sb.append(" version=");
            sb.append(version);
            sb.append(" delete?=");
            sb.append(delete);
            sb.append(" threadID=");
            sb.append(threadID);
            sb.append(" indexStartTime=");
            sb.append(indexStartTime);
            sb.append(" indexFinishTime=");
            sb.append(indexFinishTime);
            sb.append(" versionConflict=");
            sb.append(versionConflict);
            sb.append(" alreadyExists?=");
            sb.append(alreadyExists);

            if (response != null) {
                if (response instanceof DeleteResponse deleteResponse) {
                    sb.append(" response:");
                    sb.append(" index=");
                    sb.append(deleteResponse.getIndex());
                    sb.append(" id=");
                    sb.append(deleteResponse.getId());
                    sb.append(" version=");
                    sb.append(deleteResponse.getVersion());
                    sb.append(" found=");
                    sb.append(deleteResponse.getResult() == DocWriteResponse.Result.DELETED);
                } else if (response instanceof IndexResponse indexResponse) {
                    sb.append(" index=");
                    sb.append(indexResponse.getIndex());
                    sb.append(" id=");
                    sb.append(indexResponse.getId());
                    sb.append(" version=");
                    sb.append(indexResponse.getVersion());
                    sb.append(" created=");
                    sb.append(indexResponse.getResult() == DocWriteResponse.Result.CREATED);
                } else {
                    sb.append("  response: " + response);
                }
            } else {
                sb.append("  response: null");
            }

            return sb.toString();
        }
    }

    public void testRandomIDsAndVersions() throws Exception {
        createIndex("test");
        ensureGreen();

        // TODO: sometimes use _bulk API
        // TODO: test non-aborting exceptions (Rob suggested field where positions overflow)

        // TODO: not great we don't test deletes GC here:

        // We test deletes, but can't rely on wall-clock delete GC:
        updateIndexSettings(Settings.builder().put("index.gc_deletes", "1000000h"), "test");
        Random random = random();

        // Generate random IDs:
        IDSource idSource = getRandomIDs();
        Set<String> idsSet = new HashSet<>();

        String idPrefix;
        if (randomBoolean()) {
            idPrefix = "";
        } else {
            idPrefix = TestUtil.randomSimpleString(random);
            logger.debug("--> use id prefix {}", idPrefix);
        }

        int numIDs;
        if (TEST_NIGHTLY) {
            numIDs = scaledRandomIntBetween(300, 1000);
        } else {
            numIDs = scaledRandomIntBetween(50, 100);
        }

        while (idsSet.size() < numIDs) {
            idsSet.add(idPrefix + idSource.next());
        }

        String[] ids = idsSet.toArray(new String[numIDs]);

        boolean useMonotonicVersion = randomBoolean();

        // Attach random versions to them:
        long version = 0;
        final IDAndVersion[] idVersions = new IDAndVersion[TestUtil.nextInt(random, numIDs / 2, numIDs * (TEST_NIGHTLY ? 8 : 2))];
        final Map<String, IDAndVersion> truth = new HashMap<>();

        logger.debug("--> use {} ids; {} operations", numIDs, idVersions.length);

        for (int i = 0; i < idVersions.length; i++) {

            if (useMonotonicVersion) {
                version += TestUtil.nextInt(random, 1, 10);
            } else {
                version = random.nextLong() & 0x3fffffffffffffffL;
            }

            idVersions[i] = new IDAndVersion();
            idVersions[i].id = ids[random.nextInt(numIDs)];
            idVersions[i].version = version;
            // 20% of the time we delete:
            idVersions[i].delete = random.nextInt(5) == 2;
            IDAndVersion curVersion = truth.get(idVersions[i].id);
            if (curVersion == null || idVersions[i].version > curVersion.version) {
                // Save highest version per id:
                truth.put(idVersions[i].id, idVersions[i]);
            }
        }

        // Shuffle
        for (int i = idVersions.length - 1; i > 0; i--) {
            int index = random.nextInt(i + 1);
            IDAndVersion x = idVersions[index];
            idVersions[index] = idVersions[i];
            idVersions[i] = x;
        }

        for (IDAndVersion idVersion : idVersions) {
            logger.debug(
                "--> id={} version={} delete?={} truth?={}",
                idVersion.id,
                idVersion.version,
                idVersion.delete,
                truth.get(idVersion.id) == idVersion
            );
        }

        final AtomicInteger upto = new AtomicInteger();
        final CountDownLatch startingGun = new CountDownLatch(1);
        Thread[] threads = new Thread[TestUtil.nextInt(random, 1, TEST_NIGHTLY ? 20 : 5)];
        final long startTime = System.nanoTime();
        for (int i = 0; i < threads.length; i++) {
            final int threadID = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        // final Random threadRandom = RandomizedContext.current().getRandom();
                        final Random threadRandom = random();
                        startingGun.await();
                        while (true) {

                            // TODO: sometimes use bulk:

                            int index = upto.getAndIncrement();
                            if (index >= idVersions.length) {
                                break;
                            }
                            if (index % 100 == 0) {
                                logger.trace("{}: index={}", Thread.currentThread().getName(), index);
                            }
                            IDAndVersion idVersion = idVersions[index];

                            String id = idVersion.id;
                            idVersion.threadID = threadID;
                            idVersion.indexStartTime = System.nanoTime() - startTime;
                            long version = idVersion.version;
                            if (idVersion.delete) {
                                try {
                                    idVersion.response = client().prepareDelete("test", id)
                                        .setVersion(version)
                                        .setVersionType(VersionType.EXTERNAL)
                                        .get();
                                } catch (VersionConflictEngineException vcee) {
                                    // OK: our version is too old
                                    assertThat(version, lessThanOrEqualTo(truth.get(id).version));
                                    idVersion.versionConflict = true;
                                }
                            } else {
                                try {
                                    idVersion.response = prepareIndex("test").setId(id)
                                        .setSource("foo", "bar")
                                        .setVersion(version)
                                        .setVersionType(VersionType.EXTERNAL)
                                        .get();

                                } catch (VersionConflictEngineException vcee) {
                                    // OK: our version is too old
                                    assertThat(version, lessThanOrEqualTo(truth.get(id).version));
                                    idVersion.versionConflict = true;
                                }
                            }
                            idVersion.indexFinishTime = System.nanoTime() - startTime;

                            if (threadRandom.nextInt(100) == 7) {
                                logger.trace("--> {}: TEST: now refresh at {}", threadID, System.nanoTime() - startTime);
                                refresh();
                                logger.trace("--> {}: TEST: refresh done at {}", threadID, System.nanoTime() - startTime);
                            }
                            if (threadRandom.nextInt(100) == 7) {
                                logger.trace("--> {}: TEST: now flush at {}", threadID, System.nanoTime() - startTime);
                                flush();
                                logger.trace("--> {}: TEST: flush done at {}", threadID, System.nanoTime() - startTime);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[i].start();
        }

        startingGun.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify against truth:
        boolean failed = false;
        for (String id : ids) {
            long expected;
            IDAndVersion idVersion = truth.get(id);
            if (idVersion != null && idVersion.delete == false) {
                expected = idVersion.version;
            } else {
                expected = -1;
            }
            long actualVersion = client().prepareGet("test", id).get().getVersion();
            if (actualVersion != expected) {
                logger.error("--> FAILED: idVersion={} actualVersion= {}", idVersion, actualVersion);
                failed = true;
            }
        }

        if (failed) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < idVersions.length; i++) {
                sb.append("i=").append(i).append(" ").append(idVersions[i]).append(System.lineSeparator());
            }
            logger.error("All versions: {}", sb);
            fail("wrong versions for some IDs");
        }
    }

    public void testDeleteNotLost() throws Exception {

        // We require only one shard for this test, so that the 2nd delete provokes pruning the deletes map:
        indicesAdmin().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1)).get();

        ensureGreen();

        updateIndexSettings(Settings.builder().put("index.gc_deletes", "10ms").put("index.refresh_interval", "-1"), "test");

        // Index a doc:
        prepareIndex("test").setId("id")
            .setSource("foo", "bar")
            .setOpType(DocWriteRequest.OpType.INDEX)
            .setVersion(10)
            .setVersionType(VersionType.EXTERNAL)
            .get();

        if (randomBoolean()) {
            // Force refresh so the add is sometimes visible in the searcher:
            refresh();
        }

        // Delete it
        client().prepareDelete("test", "id").setVersion(11).setVersionType(VersionType.EXTERNAL).get();

        // Real-time get should reflect delete:
        assertThat("doc should have been deleted", client().prepareGet("test", "id").get().getVersion(), equalTo(-1L));

        // ThreadPool.relativeTimeInMillis has default granularity of 200 msec, so we must sleep at least that long; sleep much longer in
        // case system is busy:
        Thread.sleep(1000);

        // Delete an unrelated doc (provokes pruning deletes from versionMap)
        client().prepareDelete("test", "id2").setVersion(11).setVersionType(VersionType.EXTERNAL).get();

        // Real-time get should still reflect delete:
        assertThat("doc should have been deleted", client().prepareGet("test", "id").get().getVersion(), equalTo(-1L));
    }

    public void testGCDeletesZero() throws Exception {
        createIndex("test");
        ensureGreen();

        // We test deletes, but can't rely on wall-clock delete GC:
        updateIndexSettings(Settings.builder().put("index.gc_deletes", "0ms"), "test");
        // Index a doc:
        prepareIndex("test").setId("id")
            .setSource("foo", "bar")
            .setOpType(DocWriteRequest.OpType.INDEX)
            .setVersion(10)
            .setVersionType(VersionType.EXTERNAL)
            .get();

        if (randomBoolean()) {
            // Force refresh so the add is sometimes visible in the searcher:
            refresh();
        }

        // Delete it
        client().prepareDelete("test", "id").setVersion(11).setVersionType(VersionType.EXTERNAL).get();

        // Real-time get should reflect delete even though index.gc_deletes is 0:
        assertThat("doc should have been deleted", client().prepareGet("test", "id").get().getVersion(), equalTo(-1L));
    }

    public void testSpecialVersioning() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        DocWriteResponse doc1 = prepareIndex("test").setId("1")
            .setSource("field", "value1")
            .setVersion(0)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertThat(doc1.getVersion(), equalTo(0L));
        DocWriteResponse doc2 = prepareIndex("test").setId("1")
            .setSource("field", "value2")
            .setVersion(Versions.MATCH_ANY)
            .setVersionType(VersionType.INTERNAL)
            .get();
        assertThat(doc2.getVersion(), equalTo(1L));
        client().prepareDelete("test", "1").get(); // v2
        DocWriteResponse doc3 = prepareIndex("test").setId("1")
            .setSource("field", "value3")
            .setVersion(Versions.MATCH_DELETED)
            .setVersionType(VersionType.INTERNAL)
            .get();
        assertThat(doc3.getVersion(), equalTo(3L));
        DocWriteResponse doc4 = prepareIndex("test").setId("1")
            .setSource("field", "value4")
            .setVersion(4L)
            .setVersionType(VersionType.EXTERNAL_GTE)
            .get();
        assertThat(doc4.getVersion(), equalTo(4L));
        // Make sure that these versions are replicated correctly
        setReplicaCount(1, "test");
        ensureGreen("test");
    }
}
