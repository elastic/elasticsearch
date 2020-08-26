/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm.history;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.core.ilm.GenerateSnapshotNameStep.generateSnapshotName;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore.SLM_HISTORY_ALIAS;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore.SLM_HISTORY_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class SnapshotHistoryStoreTests extends ESTestCase {

    private ThreadPool threadPool;
    private SnapshotLifecycleTemplateRegistryTests.VerifyingClient client;
    private SnapshotHistoryStore historyStore;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new SnapshotLifecycleTemplateRegistryTests.VerifyingClient(threadPool);
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        historyStore = new SnapshotHistoryStore(Settings.EMPTY, client, clusterService);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testNoActionIfDisabled() {
        Settings settings = Settings.builder().put(SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false).build();
        SnapshotHistoryStore disabledHistoryStore = new SnapshotHistoryStore(settings, client, null);
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        String snapshotId = generateSnapshotName(policy.getName());
        SnapshotHistoryItem record = SnapshotHistoryItem.creationSuccessRecord(timestamp, policy, snapshotId);

        client.setVerifier((a, r, l) -> {
            fail("the history store is disabled, no action should have been taken");
            return null;
        });
        disabledHistoryStore.putAsync(record);
    }

    @SuppressWarnings("unchecked")
    public void testPut() throws Exception {
        String policyId = randomAlphaOfLength(5);
        SnapshotLifecyclePolicy policy = randomSnapshotLifecyclePolicy(policyId);
        final long timestamp = randomNonNegativeLong();
        String snapshotId = generateSnapshotName(policy.getName());
        {
            SnapshotHistoryItem record = SnapshotHistoryItem.creationSuccessRecord(timestamp, policy, snapshotId);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                if (action instanceof CreateIndexAction && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(IndexAction.class));
                assertThat(request, instanceOf(IndexRequest.class));
                IndexRequest indexRequest = (IndexRequest) request;
                assertEquals(SLM_HISTORY_ALIAS, indexRequest.index());
                final String indexedDocument = indexRequest.source().utf8ToString();
                assertThat(indexedDocument, containsString(policy.getId()));
                assertThat(indexedDocument, containsString(policy.getRepository()));
                assertThat(indexedDocument, containsString(snapshotId));
                if (policy.getConfig() != null) {
                    assertContainsMap(indexedDocument, policy.getConfig());
                }
                assertNotNull(listener);
                // The content of this IndexResponse doesn't matter, so just make it 100% random
                return new IndexResponse(
                    new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(100)),
                    randomAlphaOfLength(5),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomBoolean());
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }

        {
            final String cause = randomAlphaOfLength(9);
            Exception failureException = new RuntimeException(cause);
            SnapshotHistoryItem record = SnapshotHistoryItem.creationFailureRecord(timestamp, policy, snapshotId, failureException);

            AtomicInteger calledTimes = new AtomicInteger(0);
            client.setVerifier((action, request, listener) -> {
                if (action instanceof CreateIndexAction && request instanceof CreateIndexRequest) {
                    return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
                }
                calledTimes.incrementAndGet();
                assertThat(action, instanceOf(IndexAction.class));
                assertThat(request, instanceOf(IndexRequest.class));
                IndexRequest indexRequest = (IndexRequest) request;
                assertEquals(SLM_HISTORY_ALIAS, indexRequest.index());
                final String indexedDocument = indexRequest.source().utf8ToString();
                assertThat(indexedDocument, containsString(policy.getId()));
                assertThat(indexedDocument, containsString(policy.getRepository()));
                assertThat(indexedDocument, containsString(snapshotId));
                if (policy.getConfig() != null) {
                    assertContainsMap(indexedDocument, policy.getConfig());
                }
                assertThat(indexedDocument, containsString("runtime_exception"));
                assertThat(indexedDocument, containsString(cause));
                assertNotNull(listener);
                // The content of this IndexResponse doesn't matter, so just make it 100% random
                return new IndexResponse(
                    new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(100)),
                    randomAlphaOfLength(5),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomLongBetween(1, 1000),
                    randomBoolean());
            });

            historyStore.putAsync(record);
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }
    }

    public void testHistoryIndexNeedsCreation() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder())
            .build();

        client.setVerifier((a, r, l) -> {
            assertThat(a, instanceOf(CreateIndexAction.class));
            assertThat(r, instanceOf(CreateIndexRequest.class));
            CreateIndexRequest request = (CreateIndexRequest) r;
            assertThat(request.aliases(), hasSize(1));
            request.aliases().forEach(alias -> {
                assertThat(alias.name(), equalTo(SLM_HISTORY_ALIAS));
                assertTrue(alias.writeIndex());
            });
            return new CreateIndexResponse(true, true, request.index());
        });

        CountDownLatch latch = new CountDownLatch(1);
        SnapshotHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            Assert::assertTrue,
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexProperlyExistsAlready() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(SLM_HISTORY_INDEX_PREFIX + "000001")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetadata.builder(SLM_HISTORY_ALIAS)
                        .writeIndex(true)
                        .build())))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        SnapshotHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            Assert::assertFalse,
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexHasNoWriteIndex() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(SLM_HISTORY_INDEX_PREFIX + "000001")
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))
                    .putAlias(AliasMetadata.builder(SLM_HISTORY_ALIAS)
                        .build()))
            .put(IndexMetadata.builder(randomAlphaOfLength(5))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(randomIntBetween(1,10))
                .numberOfReplicas(randomIntBetween(1,10))
                .putAlias(AliasMetadata.builder(SLM_HISTORY_ALIAS)
                    .build())))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        SnapshotHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            indexCreated -> fail("should have called onFailure, not onResponse"),
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), containsString("SLM history alias [" + SLM_HISTORY_ALIAS +
                    "does not have a write index"));
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexNotAlias() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(SLM_HISTORY_ALIAS)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        SnapshotHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            indexCreated -> fail("should have called onFailure, not onResponse"),
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), containsString("SLM history alias [" + SLM_HISTORY_ALIAS +
                    "] already exists as concrete index"));
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryIndexCreatedConcurrently() throws InterruptedException {
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder())
            .build();

        client.setVerifier((a, r, l) -> {
            assertThat(a, instanceOf(CreateIndexAction.class));
            assertThat(r, instanceOf(CreateIndexRequest.class));
            CreateIndexRequest request = (CreateIndexRequest) r;
            assertThat(request.aliases(), hasSize(1));
            request.aliases().forEach(alias -> {
                assertThat(alias.name(), equalTo(SLM_HISTORY_ALIAS));
                assertTrue(alias.writeIndex());
            });
            throw new ResourceAlreadyExistsException("that index already exists");
        });

        CountDownLatch latch = new CountDownLatch(1);
        SnapshotHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            Assert::assertFalse,
            ex -> {
                logger.error(ex);
                fail("should have called onResponse, not onFailure");
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    public void testHistoryAliasDoesntExistButIndexDoes() throws InterruptedException {
        final String initialIndex = SLM_HISTORY_INDEX_PREFIX + "000001";
        ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder(initialIndex)
                    .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                    .numberOfShards(randomIntBetween(1,10))
                    .numberOfReplicas(randomIntBetween(1,10))))
            .build();

        client.setVerifier((a, r, l) -> {
            fail("no client calls should have been made");
            return null;
        });

        CountDownLatch latch = new CountDownLatch(1);
        SnapshotHistoryStore.ensureHistoryIndex(client, state, new LatchedActionListener<>(ActionListener.wrap(
            response -> {
                logger.error(response);
                fail("should have called onFailure, not onResponse");
            },
            ex -> {
                assertThat(ex, instanceOf(IllegalStateException.class));
                assertThat(ex.getMessage(), containsString("SLM history index [" + initialIndex +
                    "] already exists but does not have alias [" + SLM_HISTORY_ALIAS + "]"));
            }), latch));

        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void assertContainsMap(String indexedDocument, Map<String, Object> map) {
        map.forEach((k, v) -> {
            assertThat(indexedDocument, containsString(k));
            if (v instanceof Map) {
                assertContainsMap(indexedDocument, (Map<String, Object>) v);
            }
            if (v instanceof Iterable) {
                ((Iterable) v).forEach(elem -> {
                    assertThat(indexedDocument, containsString(elem.toString()));
                });
            } else {
                assertThat(indexedDocument, containsString(v.toString()));
            }
        });
    }

    public static SnapshotLifecyclePolicy randomSnapshotLifecyclePolicy(String id) {
        Map<String, Object> config = null;
        if (randomBoolean()) {
            config = new HashMap<>();
            for (int i = 0; i < randomIntBetween(2, 5); i++) {
                config.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
            }
        }
        return new SnapshotLifecyclePolicy(id,
            randomAlphaOfLength(4),
            randomSchedule(),
            randomAlphaOfLength(4),
            config,
            null);
    }

    private static String randomSchedule() {
        return randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 59) + " " +
            randomIntBetween(0, 12) + " * * ?";
    }
}
