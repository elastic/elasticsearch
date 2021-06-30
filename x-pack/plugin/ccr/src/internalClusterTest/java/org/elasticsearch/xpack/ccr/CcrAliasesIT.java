/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class CcrAliasesIT extends CcrIntegTestCase {

    public void testAliasOnIndexCreation() throws Exception {
        final String aliasName = randomAlphaOfLength(16);
        final String aliases;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("aliases");
                {
                    builder.startObject(aliasName);
                    {

                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            aliases = BytesReference.bytes(builder).utf8ToString();
        }
        assertAcked(leaderClient().admin().indices().prepareCreate("leader").setSource(aliases, XContentType.JSON));
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, "follower");

        // wait for the shard follow task to exist
        assertBusy(() -> assertShardFollowTask(1));

        assertAliasesExist("leader", "follower", aliasName);
    }

    public void testAddAlias() throws Exception {
        runAddAliasTest(null);
    }

    public void testAddExplicitNotWriteAlias() throws Exception {
        runAddAliasTest(false);
    }

    public void testWriteAliasIsIgnored() throws Exception {
        runAddAliasTest(true);
    }

    private void runAddAliasTest(final Boolean isWriteAlias) throws Exception {
        runAddAliasTest(isWriteAlias, aliasName -> {});
    }

    /**
     * Runs an add alias test which adds a random alias to the leader exist, and then asserts that the alias is replicated to the follower.
     * The specified post assertions gives the caller the opportunity to add additional assertions on the alias that is added. These
     * assertions are executed after all other assertions that the alias exists.
     *
     * @param isWriteIndex   whether or not the leader index is the write index for the alias
     * @param postAssertions the post assertions to execute
     * @param <E>            the type of checked exception the post assertions callback can throw
     * @throws Exception if a checked exception is thrown while executing the add alias test
     */
    private <E extends Exception> void runAddAliasTest(
            final Boolean isWriteIndex,
            final CheckedConsumer<String, E> postAssertions) throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader"));
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        // we set a low poll timeout so that shard changes requests are responded to quickly even without indexing
        followRequest.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(100));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, "follower");

        assertBusy(() -> assertShardFollowTask(1));

        final String aliasName = randomAlphaOfLength(16);
        addRandomAlias("leader", aliasName, isWriteIndex);

        assertAliasesExist("leader", "follower", aliasName);

        postAssertions.accept(aliasName);
    }

    public void testAddDataStreamAlias() throws Exception {
        runAddDataStreamAliasTest(null);
    }

    public void testAddExplicitNotWriteDataStreamAlias() throws Exception {
        runAddDataStreamAliasTest(false);
    }

    public void testWriteDataStreamAliasIsIgnored() throws Exception {
        runAddDataStreamAliasTest(true);
    }

    private void runAddDataStreamAliasTest(final Boolean isWriteIndex) throws Exception {
        try {
            // Setup component template:
            {
                PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request("id");
                request.indexTemplate(
                    new ComposableIndexTemplate(
                        List.of("logs-*"),
                        null,
                        null,
                        null,
                        null,
                        null,
                        new ComposableIndexTemplate.DataStreamTemplate(),
                        null
                    )
                );
                assertAcked(leaderClient().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet());
                assertAcked(followerClient().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet());
            }
            // Setup auto follow pattern:
            {
                PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
                request.setName("id");
                request.setRemoteCluster("leader_cluster");
                request.setLeaderIndexPatterns(List.of("logs-*"));
                FollowParameters parameters = new FollowParameters();
                // we set a low poll timeout so that shard changes requests are responded to quickly even without indexing
                parameters.setReadPollTimeout(TimeValue.timeValueMillis(100));
                request.setParameters(parameters);
                assertAcked(followerClient().execute(PutAutoFollowPatternAction.INSTANCE, request).actionGet());
            }
            // Create data stream and ensure replicated:
            {
                CreateDataStreamAction.Request request = new CreateDataStreamAction.Request("logs-foobar");
                assertAcked(leaderClient().execute(CreateDataStreamAction.INSTANCE, request).actionGet());
                ensureFollowerGreen(true, "logs-foobar");
                assertBusy(() -> assertShardFollowTask(1));
            }
            // Create data stream alias and ensure replicated:
            {
                final String aliasName = randomAlphaOfLength(16);
                final IndicesAliasesRequest.AliasActions add = IndicesAliasesRequest.AliasActions.add();
                add.index("logs-foobar");
                add.alias(aliasName);
                add.writeIndex(isWriteIndex);
                assertAcked(leaderClient().admin().indices().prepareAliases().addAliasAction(add));
                assertAliasesExist("logs-foobar", "logs-foobar", aliasName);
            }
        } finally {
            // Delete auto follow pattern:
            {
                DeleteAutoFollowPatternAction.Request request = new DeleteAutoFollowPatternAction.Request("id");
                assertAcked(followerClient().execute(DeleteAutoFollowPatternAction.INSTANCE, request).actionGet());
            }
            // Pause, close and unfollow to speed up teardown:
            {
                GetDataStreamAction.Request request = new GetDataStreamAction.Request(new String[] {"logs-foobar"});
                GetDataStreamAction.Response response = followerClient().execute(GetDataStreamAction.INSTANCE, request).actionGet();
                String index = response.getDataStreams().get(0).getDataStream().getWriteIndex().getName();
                followerClient().execute(PauseFollowAction.INSTANCE, new PauseFollowAction.Request(index)).actionGet();
                followerClient().admin().indices().close(new CloseIndexRequest(index).masterNodeTimeout(TimeValue.MAX_VALUE)).actionGet();
                followerClient().execute(UnfollowAction.INSTANCE, new UnfollowAction.Request(index)).actionGet();
            }
            // Delete data streams:
            {
                DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(new String[] {"logs-foobar"});
                assertAcked(leaderClient().execute(DeleteDataStreamAction.INSTANCE, request).actionGet());
                assertAcked(followerClient().execute(DeleteDataStreamAction.INSTANCE, request).actionGet());
            }
            // Delete composable index templates:
            {
                DeleteComposableIndexTemplateAction.Request request = new DeleteComposableIndexTemplateAction.Request("id");
                assertAcked(leaderClient().execute(DeleteComposableIndexTemplateAction.INSTANCE, request).actionGet());
                assertAcked(followerClient().execute(DeleteComposableIndexTemplateAction.INSTANCE, request).actionGet());
            }
        }
    }

    private void addRandomAlias(final String index, final String aliasName, final Boolean isWriteIndex) {
        final IndicesAliasesRequest.AliasActions add = IndicesAliasesRequest.AliasActions.add();
        add.index(index);
        add.alias(aliasName);
        add.writeIndex(isWriteIndex);
        if (randomBoolean()) {
            add.routing(randomAlphaOfLength(16));
        } else {
            if (randomBoolean()) {
                add.indexRouting(randomAlphaOfLength(16));
            }
            if (randomBoolean()) {
                add.searchRouting(randomAlphaOfLength(16));
            }
        }
        if (randomBoolean()) {
            add.filter(termQuery(randomAlphaOfLength(16), randomAlphaOfLength(16)));
        }

        assertAcked(leaderClient().admin().indices().prepareAliases().addAliasAction(add));
    }

    public void testAddMultipleAliasesAtOnce() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader"));
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        // we set a low poll timeout so that shard changes requests are responded to quickly even without indexing
        followRequest.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(100));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, "follower");

        assertBusy(() -> assertShardFollowTask(1));

        final int numberOfAliases = randomIntBetween(2, 8);
        final IndicesAliasesRequestBuilder builder = leaderClient().admin().indices().prepareAliases();
        for (int i = 0; i < numberOfAliases; i++) {
            builder.addAlias("leader", "alias_" + i);
        }
        assertAcked(builder);

        final String[] aliases = new String[numberOfAliases];
        for (int i = 0; i < numberOfAliases; i++) {
            aliases[i] = "alias_" + i;
        }
        assertAliasesExist("leader", "follower", aliases);
    }

    public void testAddMultipleAliasesSequentially() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader"));
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        // we set a low poll timeout so that shard changes requests are responded to quickly even without indexing
        followRequest.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(100));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        ensureFollowerGreen(true, "follower");

        assertBusy(() -> assertShardFollowTask(1));

        final int numberOfAliases = randomIntBetween(2, 8);
        for (int i = 0; i < numberOfAliases; i++) {
            assertAcked(leaderClient().admin().indices().prepareAliases().addAlias("leader", "alias_" + i));

            final String[] aliases = new String[i + 1];
            for (int j = 0; j < i + 1; j++) {
                aliases[j] = "alias_" + j;
            }
            assertAliasesExist("leader", "follower", aliases);
        }
    }

    public void testUpdateExistingAlias() throws Exception {
        runAddAliasTest(
                null,
                /*
                 * After the alias is added (via runAddAliasTest) we modify the alias in place, and then assert that the modification is
                 * eventually replicated.
                 */
                aliasName -> {
                    assertAcked(leaderClient().admin()
                            .indices()
                            .prepareAliases()
                            .addAlias("leader", aliasName, termQuery(randomAlphaOfLength(16), randomAlphaOfLength(16))));
                    assertAliasesExist("leader", "follower", aliasName);
                });
    }

    public void testRemoveExistingAlias() throws Exception {
        runAddAliasTest(
                false,
                aliasName -> {
                    removeAlias(aliasName);
                    assertAliasExistence("follower", aliasName, false);
                }
        );
    }

    private void removeAlias(final String aliasName) {
        assertAcked(leaderClient().admin().indices().prepareAliases().removeAlias("leader", aliasName));
    }

    public void testStress() throws Exception {
        assertAcked(leaderClient().admin().indices().prepareCreate("leader"));
        final PutFollowAction.Request followRequest = putFollow("leader", "follower");
        // we set a low poll timeout so that shard changes requests are responded to quickly even without indexing
        followRequest.getParameters().setReadPollTimeout(TimeValue.timeValueMillis(100));
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();

        final int numberOfThreads = randomIntBetween(2, 4);
        final int numberOfIterations = randomIntBetween(4, 32);
        final CyclicBarrier barrier = new CyclicBarrier(numberOfThreads + 1);
        final List<Thread> threads = new ArrayList<>(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < numberOfIterations; j++) {
                    final String action = randomFrom("create", "update", "delete");
                    switch (action) {
                        case "create":
                            addRandomAlias("leader", randomAlphaOfLength(16), randomFrom(new Boolean[] { null, false, true }));
                            break;
                        case "update":
                            try {
                                final String[] aliases = getAliasesOnLeader();
                                if (aliases.length == 0) {
                                    continue;
                                }
                                final String alias = randomFrom(aliases);
                                /*
                                 * Add an alias with the same name, which acts as an update (although another thread could concurrently
                                 * remove).
                                 */
                                addRandomAlias("leader", alias, randomFrom(new Boolean[] { null, false, true }));
                            } catch (final Exception e) {
                                throw new RuntimeException(e);
                            }
                            break;
                        case "delete":
                            try {
                                final String[] aliases = getAliasesOnLeader();
                                if (aliases.length == 0) {
                                    continue;
                                }
                                final String alias = randomFrom(aliases);
                                try {
                                    removeAlias(alias);
                                } catch (final AliasesNotFoundException e) {
                                    // ignore, it could have been deleted by another thread
                                    continue;
                                }
                            } catch (final Exception e) {
                                throw new RuntimeException(e);
                            }
                            break;
                        default:
                            assert false : action;
                    }
                }
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        barrier.await();

        barrier.await();

        for (final Thread thread : threads) {
            thread.join();
        }

        assertAliasesExist("leader", "follower", getAliasesOnLeader());
    }

    private String[] getAliasesOnLeader() throws InterruptedException, ExecutionException {
        final GetAliasesResponse response = leaderClient().admin().indices().getAliases(new GetAliasesRequest().indices("leader")).get();
        return response.getAliases().get("leader").stream().map(AliasMetadata::alias).toArray(String[]::new);
    }

    private void assertAliasesExist(final String leaderIndex, final String followerIndex, final String... aliases) throws Exception {
        assertAliasesExist(leaderIndex, followerIndex, (alias, aliasMetadata) -> {}, aliases);
    }

    private <E extends Exception> void assertAliasesExist(
            final String leaderIndex,
            final String followerIndex,
            final CheckedBiConsumer<String, AliasMetadata, E> aliasMetadataAssertion,
            final String... aliases) throws Exception {
        // we must check serially because aliases exist will return true if any but not necessarily all of the requested aliases exist
        for (final String alias : aliases) {
            assertAliasExistence(followerIndex, alias, true);
        }

        assertBusy(() -> {
            final GetAliasesResponse followerResponse =
                    followerClient().admin().indices().getAliases(new GetAliasesRequest().indices(followerIndex)).get();
            if (followerResponse.getAliases().isEmpty() == false) {
                assertThat(
                    "expected follower to have [" + aliases.length + "] aliases, but was " + followerResponse.getAliases().toString(),
                    followerResponse.getAliases().get(followerIndex),
                    hasSize(aliases.length));
                for (final String alias : aliases) {
                    final AliasMetadata followerAliasMetadata = getAliasMetadata(followerResponse, followerIndex, alias);

                    final GetAliasesResponse leaderResponse =
                        leaderClient().admin().indices().getAliases(new GetAliasesRequest().indices(leaderIndex).aliases(alias)).get();
                    final AliasMetadata leaderAliasMetadata = getAliasMetadata(leaderResponse, leaderIndex, alias);

                    assertThat(
                        "alias [" + alias + "] index routing did not replicate, but was " + followerAliasMetadata.toString(),
                        followerAliasMetadata.indexRouting(), equalTo(leaderAliasMetadata.indexRouting()));
                    assertThat(
                        "alias [" + alias + "] search routing did not replicate, but was " + followerAliasMetadata.toString(),
                        followerAliasMetadata.searchRoutingValues(), equalTo(leaderAliasMetadata.searchRoutingValues()));
                    assertThat(
                        "alias [" + alias + "] filtering did not replicate, but was " + followerAliasMetadata.toString(),
                        followerAliasMetadata.filter(), equalTo(leaderAliasMetadata.filter()));
                    assertThat(
                        "alias [" + alias + "] should not be a write index, but was " + followerAliasMetadata.toString(),
                        followerAliasMetadata.writeIndex(),
                        equalTo(false));
                    aliasMetadataAssertion.accept(alias, followerAliasMetadata);
                }
            } else if (followerResponse.getDataStreamAliases().isEmpty() == false) {
                assertThat(
                    "expected follower to have [" + aliases.length + "] aliases, but was " + followerResponse.getAliases().toString(),
                    followerResponse.getDataStreamAliases().get(followerIndex),
                    hasSize(aliases.length));
                for (final String alias : aliases) {
                    final DataStreamAlias followerDataStreamAlias = getDataStreamAlias(followerResponse, followerIndex, alias);

                    final GetAliasesResponse leaderResponse =
                        leaderClient().admin().indices().getAliases(new GetAliasesRequest().indices(leaderIndex).aliases(alias)).get();
                    final DataStreamAlias leaderDataStreamAlias = getDataStreamAlias(leaderResponse, leaderIndex, alias);

                    assertThat(
                        "alias [" + alias + "] data streams did not replicate, but was " + followerDataStreamAlias,
                        followerDataStreamAlias.getDataStreams(), equalTo(leaderDataStreamAlias.getDataStreams()));
                    assertThat(
                        "alias [" + alias + "] should not be a write data stream, but was " + followerDataStreamAlias,
                        followerDataStreamAlias.getWriteDataStream(),
                        nullValue());
                }
            } else {
                fail("no indices or data stream aliases were found");
            }
        });
    }

    private void assertAliasExistence(final String index, final String alias, final boolean exists) throws Exception {
        assertBusy(() -> {
            // we must check serially because aliases exist will return true if any but not necessarily all of the requested aliases exist
            final GetAliasesResponse response = followerClient().admin()
                    .indices()
                    .getAliases(new GetAliasesRequest().indices(index).aliases(alias))
                    .actionGet();
            if (exists) {
                assertFalse("alias [" + alias + "] did not exist",
                    response.getAliases().isEmpty() && response.getDataStreamAliases().isEmpty());
            } else {
                assertTrue("alias [" + alias + "] exists", response.getAliases().isEmpty() && response.getDataStreamAliases().isEmpty());
            }
        });
    }

    private AliasMetadata getAliasMetadata(final GetAliasesResponse response, final String index, final String alias) {
        final Optional<AliasMetadata> maybeAliasMetadata =
                response.getAliases().get(index).stream().filter(a -> a.getAlias().equals(alias)).findFirst();
        assertTrue("alias [" + alias + "] did not exist", maybeAliasMetadata.isPresent());
        return maybeAliasMetadata.get();
    }

    private DataStreamAlias getDataStreamAlias(final GetAliasesResponse response, final String index, final String alias) {
        final Optional<DataStreamAlias> maybeAliasMetadata =
            response.getDataStreamAliases().get(index).stream().filter(a -> a.getName().equals(alias)).findFirst();
        assertTrue("alias [" + alias + "] did not exist", maybeAliasMetadata.isPresent());
        return maybeAliasMetadata.get();
    }

    private CheckedRunnable<Exception> assertShardFollowTask(final int numberOfPrimaryShards) {
        return () -> {
            final ClusterState clusterState = followerClient().admin().cluster().prepareState().get().getState();
            final PersistentTasksCustomMetadata taskMetadata = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            assertNotNull("task metadata for follower should exist", taskMetadata);

            final ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setDetailed(true);
            listTasksRequest.setActions(ShardFollowTask.NAME + "[c]");
            final ListTasksResponse listTasksResponse = followerClient().admin().cluster().listTasks(listTasksRequest).actionGet();
            assertThat("expected no node failures", listTasksResponse.getNodeFailures().size(), equalTo(0));
            assertThat("expected no task failures", listTasksResponse.getTaskFailures().size(), equalTo(0));

            final List<TaskInfo> taskInfos = listTasksResponse.getTasks();
            assertThat("expected a task for each shard", taskInfos.size(), equalTo(numberOfPrimaryShards));
            final Collection<PersistentTasksCustomMetadata.PersistentTask<?>> shardFollowTasks =
                    taskMetadata.findTasks(ShardFollowTask.NAME, Objects::nonNull);
            for (final PersistentTasksCustomMetadata.PersistentTask<?> shardFollowTask : shardFollowTasks) {
                TaskInfo taskInfo = null;
                final String expectedId = "id=" + shardFollowTask.getId();
                for (final TaskInfo info : taskInfos) {
                    if (expectedId.equals(info.getDescription())) {
                        taskInfo = info;
                        break;
                    }
                }
                assertNotNull("task info for shard follow task [" + expectedId + "] should exist", taskInfo);
            }
        };
    }

}
