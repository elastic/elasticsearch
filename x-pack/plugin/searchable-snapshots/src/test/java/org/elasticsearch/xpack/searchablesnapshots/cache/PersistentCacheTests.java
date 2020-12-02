/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.elasticsearch.index.store.cache.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.PersistentCache.createCacheIndexWriter;
import static org.elasticsearch.xpack.searchablesnapshots.cache.PersistentCache.resolveCacheIndexFolder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentCacheTests extends AbstractSearchableSnapshotsTestCase {

    public void testCacheIndexWriter() throws Exception {
        final NodeEnvironment.NodePath nodePath = randomFrom(nodeEnvironment.nodePaths());

        int docId = 0;
        final Map<String, Integer> liveDocs = new HashMap<>();
        final Set<String> deletedDocs = new HashSet<>();

        for (int iter = 0; iter < 20; iter++) {

            final Path snapshotCacheIndexDir = resolveCacheIndexFolder(nodePath);
            assertThat(Files.exists(snapshotCacheIndexDir), equalTo(iter > 0));

            try (PersistentCache.CacheIndexWriter writer = createCacheIndexWriter(nodePath)) {
                assertThat(writer.nodePath(), sameInstance(nodePath));
                assertThat(writer.getDocuments(), notNullValue());
                assertThat(writer.getDocuments().size(), equalTo(liveDocs.size()));

                // verify that existing documents are loaded
                for (Map.Entry<String, Integer> liveDoc : liveDocs.entrySet()) {
                    final Document document = writer.getDocument(liveDoc.getKey());
                    assertThat("Document should be loaded", document, notNullValue());
                    final String iteration = document.get("update_iteration");
                    assertThat(iteration, equalTo(String.valueOf(liveDoc.getValue())));
                    writer.updateCacheFile(liveDoc.getKey(), document);
                }

                // verify that deleted documents are not loaded
                for (String deletedDoc : deletedDocs) {
                    final Document document = writer.getDocument(deletedDoc);
                    assertThat("Document should not be loaded", document, nullValue());
                }

                // random updates of existing documents
                final Map<String, Integer> updatedDocs = new HashMap<>();
                for (String cacheId : randomSubsetOf(liveDocs.keySet())) {
                    final Document document = new Document();
                    document.add(new StringField("cache_id", cacheId, Field.Store.YES));
                    document.add(new StringField("update_iteration", String.valueOf(iter), Field.Store.YES));
                    writer.updateCacheFile(cacheId, document);

                    updatedDocs.put(cacheId, iter);
                }

                // create new random documents
                final Map<String, Integer> newDocs = new HashMap<>();
                for (int i = 0; i < between(1, 10); i++) {
                    final String cacheId = String.valueOf(docId++);
                    final Document document = new Document();
                    document.add(new StringField("cache_id", cacheId, Field.Store.YES));
                    document.add(new StringField("update_iteration", String.valueOf(iter), Field.Store.YES));
                    writer.updateCacheFile(cacheId, document);

                    newDocs.put(cacheId, iter);
                }

                // deletes random documents
                final Map<String, Integer> removedDocs = new HashMap<>();
                for (String cacheId : randomSubsetOf(Sets.union(liveDocs.keySet(), newDocs.keySet()))) {
                    writer.deleteCacheFile(cacheId);

                    removedDocs.put(cacheId, iter);
                }

                boolean commit = false;
                if (randomBoolean()) {
                    writer.prepareCommit();
                    if (randomBoolean()) {
                        writer.commit();
                        commit = true;
                    }
                }

                if (commit) {
                    assertThat(writer.getDocuments(), nullValue());
                    liveDocs.putAll(updatedDocs);
                    liveDocs.putAll(newDocs);
                    for (String cacheId : removedDocs.keySet()) {
                        liveDocs.remove(cacheId);
                        deletedDocs.add(cacheId);
                    }
                }
            }
        }
    }

    private static final byte[] buffer;
    static {
        buffer = new byte[1024];
        Arrays.fill(buffer, (byte) 0xff);
    }

    public void testCleanUp() throws Exception {
        final List<Path> cacheFiles = new ArrayList<>();
        try (CacheService cacheService = defaultCacheService()) {
            cacheService.start();

            for (int snapshots = 0; snapshots < between(1, 2); snapshots++) {
                SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()));
                for (int indices = 0; indices < between(1, 2); indices++) {
                    IndexId indexId = new IndexId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()));
                    for (int shards = 0; shards < between(1, 2); shards++) {
                        ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), shards);

                        final Path cacheDir = Files.createDirectories(
                            CacheService.resolveSnapshotCache(randomShardPath(shardId)).resolve(snapshotId.getUUID())
                        );
                        cacheFiles.add(cacheDir);

                        for (int files = 0; files < between(1, 2); files++) {
                            final CacheKey cacheKey = new CacheKey(snapshotId, indexId, shardId, "file_" + files);
                            final CacheFile cacheFile = cacheService.get(cacheKey, randomLongBetween(0L, buffer.length), cacheDir);

                            final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                            cacheFile.acquire(listener);
                            try {
                                randomPopulateAndReads(cacheFile, (channel, from, to) -> {
                                    try {
                                        channel.write(ByteBuffer.wrap(buffer, Math.toIntExact(from), Math.toIntExact(to)));
                                    } catch (IOException e) {
                                        throw new AssertionError(e);
                                    }
                                });
                                cacheFiles.add(cacheFile.getFile());
                            } finally {
                                cacheFile.release(listener);
                            }
                        }
                    }
                }
            }
            if (randomBoolean()) {
                cacheService.synchronizeCache();
            }
        }

        final Settings nodeSettings = Settings.builder()
            .put(NODE_ROLES_SETTING.getKey(), randomValueOtherThan(DATA_ROLE, () -> randomFrom(BUILT_IN_ROLES)).roleName())
            .build();

        assertTrue(cacheFiles.stream().allMatch(Files::exists));
        PersistentCache.cleanUp(nodeSettings, nodeEnvironment);
        assertTrue(cacheFiles.stream().noneMatch(Files::exists));
    }
}
