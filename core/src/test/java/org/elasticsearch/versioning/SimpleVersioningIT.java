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
package org.elasticsearch.versioning;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.FlushNotAllowedEngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 *
 */
public class SimpleVersioningIT extends ESIntegTestCase {

    @Test
    public void testExternalVersioningInitialDelete() throws Exception {
        createIndex("test");
        ensureGreen();

        // Note - external version doesn't throw version conflicts on deletes of non existent records. This is different from internal versioning

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));

        // this should conflict with the delete command transaction which told us that the object was deleted at version 17.
        assertThrows(
                client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
                VersionConflictEngineException.class
        );

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(18).
                setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(18L));
    }

    @Test
    public void testForce() throws Exception {
        createIndex("test");
        ensureGreen("test"); // we are testing force here which doesn't work if we are recovering at the same time - zzzzz...
        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(12).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(14).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.FORCE).get();
        assertThat(indexResponse.getVersion(), equalTo(13l));

        client().admin().indices().prepareRefresh().execute().actionGet();
        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(13l));
        }

        // deleting with a lower version works.
        long v= randomIntBetween(12,14);
        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(v).setVersionType(VersionType.FORCE).get();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(v));
    }

    @Test
    public void testExternalGTE() throws Exception {
        createIndex("test");

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(12).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(14).setVersionType(VersionType.EXTERNAL_GTE).get();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL_GTE),
                VersionConflictEngineException.class);

        client().admin().indices().prepareRefresh().execute().actionGet();
        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").get().getVersion(), equalTo(14l));
        }

        // deleting with a lower version fails.
        assertThrows(
                client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL_GTE),
                VersionConflictEngineException.class);

        // Delete with a higher or equal version deletes all versions up to the given one.
        long v= randomIntBetween(14,17);
        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(v).setVersionType(VersionType.EXTERNAL_GTE).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(v));

        // Deleting with a lower version keeps on failing after a delete.
        assertThrows(
                client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL_GTE).execute(),
                VersionConflictEngineException.class);


        // But delete with a higher version is OK.
        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(18).setVersionType(VersionType.EXTERNAL_GTE).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(18l));
    }

    @Test
    public void testExternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(12).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(12l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(14).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(14l));

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(13).setVersionType(VersionType.EXTERNAL).execute(),
                     VersionConflictEngineException.class);

        if (randomBoolean()) {
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(14l));
        }

        // deleting with a lower version fails.
        assertThrows(
            client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class);

        // Delete with a higher version deletes all versions up to the given one.
        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(17).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(17l));

        // Deleting with a lower version keeps on failing after a delete.
        assertThrows(
            client().prepareDelete("test", "type", "1").setVersion(2).setVersionType(VersionType.EXTERNAL).execute(),
            VersionConflictEngineException.class);


        // But delete with a higher version is OK.
        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(18).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(18l));


        // TODO: This behavior breaks rest api returning http status 201, good news is that it this is only the case until deletes GC kicks in.
        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(19).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(19l));


        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(20).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(20l));

        // Make sure that the next delete will be GC. Note we do it on the index settings so it will be cleaned up
        HashMap<String,Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes",-1);
        client().admin().indices().prepareUpdateSettings("test").setSettings(newSettings).execute().actionGet();

        Thread.sleep(300); // gc works based on estimated sampled time. Give it a chance...

        // And now we have previous version return -1
        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(20).setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(20l));
    }

    @Test
    public void testRequireUnitsOnUpdateSettings() throws Exception {
        createIndex("test");
        ensureGreen();
        HashMap<String,Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes", "42");
        try {
            client().admin().indices().prepareUpdateSettings("test").setSettings(newSettings).execute().actionGet();
            fail("did not hit expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
            assertTrue(iae.getMessage().contains("Failed to parse setting [index.gc_deletes] with value [42] as a time value: unit is missing or unrecognized"));
        }
    }

    @Test
    public void testInternalVersioningInitialDelete() throws Exception {
        createIndex("test");
        ensureGreen();

        assertThrows(client().prepareDelete("test", "type", "1").setVersion(17).execute(),
                VersionConflictEngineException.class);

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1")
                .setCreate(true).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));
    }


    @Test
    public void testInternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));

        assertThrows(
                client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(
            client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
            VersionConflictEngineException.class);

        assertThrows(
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);
        assertThrows(
            client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(
                client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(2).execute(),
                DocumentAlreadyExistsException.class);
        assertThrows(
                client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(2).execute(),
                DocumentAlreadyExistsException.class);


        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);
        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(2l));
        }

        // search with versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(2l));
        }

        // search without versioning
        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(Versions.NOT_FOUND));
        }

        DeleteResponse deleteResponse = client().prepareDelete("test", "type", "1").setVersion(2).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));
        assertThat(deleteResponse.getVersion(), equalTo(3l));

        assertThrows(client().prepareDelete("test", "type", "1").setVersion(2).execute(), VersionConflictEngineException.class);


        // This is intricate - the object was deleted but a delete transaction was with the right version. We add another one
        // and thus the transaction is increased.
        deleteResponse = client().prepareDelete("test", "type", "1").setVersion(3).execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(false));
        assertThat(deleteResponse.getVersion(), equalTo(4l));
    }

    @Test
    public void testSimpleVersioningWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        client().admin().indices().prepareFlush().execute().actionGet();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").setVersion(1).execute().actionGet();
        assertThat(indexResponse.getVersion(), equalTo(2l));

        client().admin().indices().prepareFlush().execute().actionGet();

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);
        assertThrows(client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client().prepareIndex("test", "type", "1").setCreate(true).setSource("field1", "value1_1").setVersion(1).execute(),
                VersionConflictEngineException.class);

        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);
        assertThrows(client().prepareDelete("test", "type", "1").setVersion(1).execute(), VersionConflictEngineException.class);

        client().admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            assertThat(client().prepareGet("test", "type", "1").execute().actionGet().getVersion(), equalTo(2l));
        }

        for (int i = 0; i < 10; i++) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setVersion(true).execute().actionGet();
            assertThat(searchResponse.getHits().getAt(0).version(), equalTo(2l));
        }
    }

    @Test
    public void testVersioningWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk().add(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1")).execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertThat(indexResponse.getVersion(), equalTo(1l));
    }


    // Poached from Lucene's TestIDVersionPostingsFormat:

    private interface IDSource {
        String next();
    }

    private IDSource getRandomIDs() {
        IDSource ids;
        final Random random = getRandom();
        switch (random.nextInt(6)) {
        case 0:
            // random simple
            if (VERBOSE) {
                System.out.println("TEST: use random simple ids");
            }
            ids = new IDSource() {
                    @Override
                    public String next() {
                        return TestUtil.randomSimpleString(random);
                    }
                };
            break;
        case 1:
            // random realistic unicode
            if (VERBOSE) {
                System.out.println("TEST: use random realistic unicode ids");
            }
            ids = new IDSource() {
                    @Override
                    public String next() {
                        return TestUtil.randomRealisticUnicodeString(random);
                    }
                };
            break;
        case 2:
            // sequential
            if (VERBOSE) {
                System.out.println("TEST: use seuquential ids");
            }
            ids = new IDSource() {
                    int upto;
                    @Override
                    public String next() {
                        return Integer.toString(upto++);
                    }
                };
            break;
        case 3:
            // zero-pad sequential
            if (VERBOSE) {
                System.out.println("TEST: use zero-pad seuquential ids");
            }
            ids = new IDSource() {
                    final int radix = TestUtil.nextInt(random, Character.MIN_RADIX, Character.MAX_RADIX);
                    final String zeroPad = String.format(Locale.ROOT, "%0" + TestUtil.nextInt(random, 4, 20) + "d", 0);
                    int upto;
                    @Override
                    public String next() {
                        String s = Integer.toString(upto++);
                        return zeroPad.substring(zeroPad.length() - s.length()) + s;
                    }
                };
            break;
        case 4:
            // random long
            if (VERBOSE) {
                System.out.println("TEST: use random long ids");
            }
            ids = new IDSource() {
                    final int radix = TestUtil.nextInt(random, Character.MIN_RADIX, Character.MAX_RADIX);
                    int upto;
                    @Override
                    public String next() {
                        return Long.toString(random.nextLong() & 0x3ffffffffffffffL, radix);
                    }
                };
            break;
        case 5:
            // zero-pad random long
            if (VERBOSE) {
                System.out.println("TEST: use zero-pad random long ids");
            }
            ids = new IDSource() {
                    final int radix = TestUtil.nextInt(random, Character.MIN_RADIX, Character.MAX_RADIX);
                    final String zeroPad = String.format(Locale.ROOT, "%015d", 0);
                    int upto;
                    @Override
                    public String next() {
                        return Long.toString(random.nextLong() & 0x3ffffffffffffffL, radix);
                    }
                };
            break;
        default:
            throw new AssertionError();
        }

        return ids;
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
                if (response instanceof DeleteResponse) {
                    DeleteResponse deleteResponse = (DeleteResponse) response;
                    sb.append(" response:");
                    sb.append(" index=");
                    sb.append(deleteResponse.getIndex());
                    sb.append(" id=");
                    sb.append(deleteResponse.getId());
                    sb.append(" type=");
                    sb.append(deleteResponse.getType());
                    sb.append(" version=");
                    sb.append(deleteResponse.getVersion());
                    sb.append(" found=");
                    sb.append(deleteResponse.isFound());
                } else if (response instanceof IndexResponse) {
                    IndexResponse indexResponse = (IndexResponse) response;
                    sb.append(" index=");
                    sb.append(indexResponse.getIndex());
                    sb.append(" id=");
                    sb.append(indexResponse.getId());
                    sb.append(" type=");
                    sb.append(indexResponse.getType());
                    sb.append(" version=");
                    sb.append(indexResponse.getVersion());
                    sb.append(" created=");
                    sb.append(indexResponse.isCreated());
                } else {
                    sb.append("  response: " + response);
                }
            } else {
                sb.append("  response: null");
            }
            
            return sb.toString();
        }
    }


    @Test
    public void testRandomIDsAndVersions() throws Exception {
        createIndex("test");
        ensureGreen();

        // TODO: sometimes use _bulk API
        // TODO: test non-aborting exceptions (Rob suggested field where positions overflow)

        // TODO: not great we don't test deletes GC here:

        // We test deletes, but can't rely on wall-clock delete GC:
        HashMap<String,Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes", "1000000h");
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(newSettings).execute().actionGet());

        Random random = getRandom();

        // Generate random IDs:
        IDSource idSource = getRandomIDs();
        Set<String> idsSet = new HashSet<>();

        String idPrefix;
        if (randomBoolean()) {
            idPrefix = "";
        } else {
            idPrefix = TestUtil.randomSimpleString(random);
            if (VERBOSE) {
                System.out.println("TEST: use id prefix: " + idPrefix);
            }
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
        final IDAndVersion[] idVersions = new IDAndVersion[TestUtil.nextInt(random, numIDs/2, numIDs*(TEST_NIGHTLY ? 8 : 2))];
        final Map<String,IDAndVersion> truth = new HashMap<>();

        if (VERBOSE) {
            System.out.println("TEST: use " + numIDs + " ids; " + idVersions.length + " operations");
        }

        for(int i=0;i<idVersions.length;i++) {

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
        for(int i = idVersions.length - 1; i > 0; i--) {
            int index = random.nextInt(i + 1);
            IDAndVersion x = idVersions[index];
            idVersions[index] = idVersions[i];
            idVersions[i] = x;
        }

        if (VERBOSE) {
            for(IDAndVersion idVersion : idVersions) {
                System.out.println("id=" + idVersion.id + " version=" + idVersion.version + " delete?=" + idVersion.delete + " truth?=" + (truth.get(idVersion.id) == idVersion));
            }
        }

        final AtomicInteger upto = new AtomicInteger();
        final CountDownLatch startingGun = new CountDownLatch(1);
        Thread[] threads = new Thread[TestUtil.nextInt(random, 1, TEST_NIGHTLY ? 20 : 5)];
        final long startTime = System.nanoTime();
        for(int i=0;i<threads.length;i++) {
            final int threadID = i;
            threads[i] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            //final Random threadRandom = RandomizedContext.current().getRandom();
                            final Random threadRandom = getRandom();
                            startingGun.await();
                            while (true) {

                                // TODO: sometimes use bulk:

                                int index = upto.getAndIncrement();
                                if (index >= idVersions.length) {
                                    break;
                                }
                                if (VERBOSE && index % 100 == 0) {
                                    System.out.println(Thread.currentThread().getName() + ": index=" + index);
                                }
                                IDAndVersion idVersion = idVersions[index];

                                String id = idVersion.id;
                                idVersion.threadID = threadID;
                                idVersion.indexStartTime = System.nanoTime()-startTime;
                                long version = idVersion.version;
                                if (idVersion.delete) {
                                    try {
                                        idVersion.response = client().prepareDelete("test", "type", id)
                                            .setVersion(version)
                                            .setVersionType(VersionType.EXTERNAL).execute().actionGet();
                                    } catch (VersionConflictEngineException vcee) {
                                        // OK: our version is too old
                                        assertThat(version, lessThanOrEqualTo(truth.get(id).version));
                                        idVersion.versionConflict = true;
                                    }
                                } else {
                                    for (int x=0;x<2;x++) {
                                        // Try create first:
                                    
                                        IndexRequest.OpType op;
                                        if (x == 0) {
                                            op = IndexRequest.OpType.CREATE;
                                        } else {
                                            op = IndexRequest.OpType.INDEX;
                                        }
                                    
                                        // index document
                                        try {
                                            idVersion.response = client().prepareIndex("test", "type", id)
                                                .setSource("foo", "bar")
                                                .setOpType(op)
                                                .setVersion(version)
                                                .setVersionType(VersionType.EXTERNAL).execute().actionGet();
                                            break;
                                        } catch (DocumentAlreadyExistsException daee) {
                                            if (x == 0) {
                                                // OK: id was already indexed by another thread, now use index:
                                                idVersion.alreadyExists = true;
                                            } else {
                                                // Should not happen with op=INDEX:
                                                throw daee;
                                            }
                                        } catch (VersionConflictEngineException vcee) {
                                            // OK: our version is too old
                                            assertThat(version, lessThanOrEqualTo(truth.get(id).version));
                                            idVersion.versionConflict = true;
                                        }
                                    }
                                }
                                idVersion.indexFinishTime = System.nanoTime()-startTime;

                                if (threadRandom.nextInt(100) == 7) {
                                    System.out.println(threadID + ": TEST: now refresh at " + (System.nanoTime()-startTime));
                                    refresh();
                                    System.out.println(threadID + ": TEST: refresh done at " + (System.nanoTime()-startTime));
                                }
                                if (threadRandom.nextInt(100) == 7) {
                                    System.out.println(threadID + ": TEST: now flush at " + (System.nanoTime()-startTime));
                                    try {
                                        flush();
                                    } catch (FlushNotAllowedEngineException fnaee) {
                                        // OK
                                    }
                                    System.out.println(threadID + ": TEST: flush done at " + (System.nanoTime()-startTime));
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
        for(Thread thread : threads) {
            thread.join();
        }

        // Verify against truth:
        boolean failed = false;
        for(String id : ids) {
            long expected;
            IDAndVersion idVersion = truth.get(id);
            if (idVersion != null && idVersion.delete == false) {
                expected = idVersion.version;
            } else {
                expected = -1;
            }
            long actualVersion = client().prepareGet("test", "type", id).execute().actionGet().getVersion();
            if (actualVersion != expected) {
                System.out.println("FAILED: idVersion=" + idVersion + " actualVersion=" + actualVersion);
                failed = true;
            }
        }

        if (failed) {
            System.out.println("All versions:");
            for(int i=0;i<idVersions.length;i++) {
                System.out.println("i=" + i + " " + idVersions[i]);
            }
            fail("wrong versions for some IDs");
        }
    }

    @Test
    public void testDeleteNotLost() throws Exception {

        // We require only one shard for this test, so that the 2nd delete provokes pruning the deletes map:
        client()
            .admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.settingsBuilder()
                         .put("index.number_of_shards", 1))
            .execute().
            actionGet();

        ensureGreen();

        HashMap<String,Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes", "10ms");
        newSettings.put("index.refresh_interval", "-1");
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(newSettings)
            .execute()
            .actionGet();

        // Index a doc:
        client()
            .prepareIndex("test", "type", "id")
            .setSource("foo", "bar")
            .setOpType(IndexRequest.OpType.INDEX)
            .setVersion(10)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();

        if (randomBoolean()) {
            // Force refresh so the add is sometimes visible in the searcher:
            refresh();
        }

        // Delete it
        client()
            .prepareDelete("test", "type", "id")
            .setVersion(11)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();

        // Real-time get should reflect delete:
        assertThat("doc should have been deleted",
                   client()
                   .prepareGet("test", "type", "id")
                   .execute()
                   .actionGet()
                   .getVersion(),
                   equalTo(-1L));

        // ThreadPool.estimatedTimeInMillis has default granularity of 200 msec, so we must sleep at least that long; sleep much longer in
        // case system is busy:
        Thread.sleep(1000);

        // Delete an unrelated doc (provokes pruning deletes from versionMap)
        client()
            .prepareDelete("test", "type", "id2")
            .setVersion(11)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();

        // Real-time get should still reflect delete:
        assertThat("doc should have been deleted",
                   client()
                   .prepareGet("test", "type", "id")
                   .execute()
                   .actionGet()
                   .getVersion(),
                   equalTo(-1L));
    }

    @Test
    public void testGCDeletesZero() throws Exception {

        createIndex("test");
        ensureGreen();

        // We test deletes, but can't rely on wall-clock delete GC:
        HashMap<String,Object> newSettings = new HashMap<>();
        newSettings.put("index.gc_deletes", "0ms");
        client()
            .admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(newSettings)
            .execute()
            .actionGet();

        // Index a doc:
        client()
            .prepareIndex("test", "type", "id")
            .setSource("foo", "bar")
            .setOpType(IndexRequest.OpType.INDEX)
            .setVersion(10)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();

        if (randomBoolean()) {
            // Force refresh so the add is sometimes visible in the searcher:
            refresh();
        }

        // Delete it
        client()
            .prepareDelete("test", "type", "id")
            .setVersion(11)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();

        // Real-time get should reflect delete even though index.gc_deletes is 0:
        assertThat("doc should have been deleted",
                   client()
                   .prepareGet("test", "type", "id")
                   .execute()
                   .actionGet()
                   .getVersion(),
                   equalTo(-1L));
    }
}
