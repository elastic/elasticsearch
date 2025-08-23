/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for the {@link ForceMergeAction}. These tests create a data stream or an index with an alias and attach an ILM policy
 * with a force merge action to it. The tests wait for the force merge action to complete and then verify that the force merged index has
 * the expected number of segments, codec, number of shards and replicas, and that the data stream or alias is updated correctly.
 */
@TestLogging(
    reason = "Enabling ILM trace logs to aid potential future debugging",
    value = "org.elasticsearch.xpack.ilm:TRACE,org.elasticsearch.xpack.core.ilm:TRACE"
)
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class ForceMergeActionIT extends ESIntegTestCase {
    // TODO: test security on cloned index

    private String policy;

    @Before
    public void refreshAbstractions() {
        policy = "policy-" + randomAlphaOfLength(5);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s")
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .build();
    }

    /**
     * Tests a force merge action on a rolled over data stream index, without specifying an index codec.
     */
    public void testDataStreamWithoutCodec() {
        String dataStream = "logs-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with data stream [{}] and policy [{}]", getTestName(), dataStream, policy);

        ForceMergeAction forceMergeAction = new ForceMergeAction(1, null);
        createSingletonPolicy(forceMergeAction);
        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = 1;
        createDataStream(dataStream, primaries, replicas);
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStream).getFirst();
        awaitStep(firstBackingIndex, "hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);

        indexAndRollover(dataStream);
        String forceMergedIndex = awaitForceMergedIndexName(firstBackingIndex);
        assertDataStreamForceMergeComplete(forceMergedIndex, dataStream, false, primaries, replicas);
    }

    /**
     * Tests a force merge action on a rolled over data stream index, with specifying the best compression index codec.
     */
    public void testDataStreamWithCodec() {
        String dataStream = "logs-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with data stream [{}] and policy [{}]", getTestName(), dataStream, policy);

        ForceMergeAction forceMergeAction = new ForceMergeAction(1, CodecService.BEST_COMPRESSION_CODEC);
        createSingletonPolicy(forceMergeAction);
        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = 1;
        createDataStream(dataStream, primaries, replicas);
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStream).getFirst();
        awaitStep(firstBackingIndex, "hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);

        indexAndRollover(dataStream);
        String forceMergedIndex = awaitForceMergedIndexName(firstBackingIndex);
        assertDataStreamForceMergeComplete(forceMergedIndex, dataStream, true, primaries, replicas);
    }

    /**
     * Tests a force merge action on a rolled over data stream index with 0 replicas, which causes the force merge to be done in place
     * instead of cloning the index.
     */
    public void testDataStreamZeroReplicas() {
        String dataStream = "logs-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with data stream [{}] and policy [{}]", getTestName(), dataStream, policy);

        // We don't care about the codec in this test, so just randomize it
        var codec = randomBoolean() ? CodecService.BEST_COMPRESSION_CODEC : null;
        ForceMergeAction forceMergeAction = new ForceMergeAction(1, codec);
        createSingletonPolicy(forceMergeAction);
        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = 0;
        createDataStream(dataStream, primaries, replicas);
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStream).getFirst();
        awaitStep(firstBackingIndex, "hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);

        indexAndRollover(dataStream);
        assertDataStreamForceMergeComplete(firstBackingIndex, dataStream, codec != null, primaries, replicas);
    }

    /**
     * Tests a force merge action on a rolled over data stream index, while doing a rolling restart of the cluster.
     * The rolling restart is done either before or after the force merge index is created, to test both cases.
     */
    public void testDataStreamRollingRestart() throws Exception {
        String dataStream = "logs-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with data stream [{}] and policy [{}]", getTestName(), dataStream, policy);

        ForceMergeAction forceMergeAction = new ForceMergeAction(1, null);
        createSingletonPolicy(forceMergeAction);
        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = 1;
        createDataStream(dataStream, primaries, replicas);
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStream).getFirst();
        awaitStep(firstBackingIndex, "hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);

        indexAndRollover(dataStream);
        boolean upgradeBeforeForceMerge = randomBoolean();
        logger.info("--> performing full restart " + (upgradeBeforeForceMerge ? "before" : "after") + " force merge index found");
        if (upgradeBeforeForceMerge) {
            internalCluster().rollingRestart(InternalTestCluster.EMPTY_CALLBACK);
        }
        String forceMergedIndex = awaitForceMergedIndexName(firstBackingIndex);
        if (upgradeBeforeForceMerge == false) {
            internalCluster().rollingRestart(InternalTestCluster.EMPTY_CALLBACK);
        }

        assertDataStreamForceMergeComplete(forceMergedIndex, dataStream, false, primaries, replicas);
    }

    /**
     * Tests a force merge action on an index with an alias, without specifying an index codec.
     */
    public void testAliasWithoutCodec() {
        String index = "index-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "-000001";
        String alias = "alias-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with index [{}], alias [{}], and policy [{}]", getTestName(), index, alias, policy);

        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = randomIntBetween(1, internalCluster().numDataNodes() - 1);

        ForceMergeAction forceMergeAction = new ForceMergeAction(1, null);
        createSingletonPolicy(forceMergeAction);
        createIndexWithAlias(index, alias, primaries, replicas);

        indexAndRollover(alias);
        String forceMergedIndex = awaitForceMergedIndexName(index);
        assertAliasForceMergeComplete(forceMergedIndex, alias, false, primaries, replicas);
    }

    /**
     * Tests a force merge action on an index with an alias, with specifying the best compression index codec.
     */
    public void testAliasWithCodec() {
        String index = "index-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "-000001";
        String alias = "alias-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with index [{}], alias [{}], and policy [{}]", getTestName(), index, alias, policy);

        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        ForceMergeAction forceMergeAction = new ForceMergeAction(1, CodecService.BEST_COMPRESSION_CODEC);
        createSingletonPolicy(forceMergeAction);
        createIndexWithAlias(index, alias, primaries, replicas);

        indexAndRollover(alias);
        String forceMergedIndex = awaitForceMergedIndexName(index);
        assertAliasForceMergeComplete(forceMergedIndex, alias, true, primaries, replicas);
    }

    /**
     * Tests a force merge action on an index with an alias, with specifying the best compression index codec.
     */
    public void testAliasZeroReplicas() {
        String index = "index-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "-000001";
        String alias = "alias-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with index [{}], alias [{}], and policy [{}]", getTestName(), index, alias, policy);

        int primaries = randomIntBetween(1, internalCluster().numDataNodes() - 1);
        int replicas = 0;
        // We don't care about the codec in this test, so just randomize it.
        var codec = randomBoolean() ? CodecService.BEST_COMPRESSION_CODEC : null;
        ForceMergeAction forceMergeAction = new ForceMergeAction(1, codec);
        createSingletonPolicy(forceMergeAction);
        createIndexWithAlias(index, alias, primaries, replicas);

        indexAndRollover(alias);
        assertAliasForceMergeComplete(index, alias, codec != null, primaries, replicas);
    }

    /**
     * Creates a singleton ILM policy with the given force merge action, and a rollover action that rolls over at 2 primary shard documents.
     */
    private void createSingletonPolicy(ForceMergeAction forceMergeAction) {
        RolloverAction rolloverAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Map<String, Phase> phases = Map.of(
            "hot",
            new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, rolloverAction, ForceMergeAction.NAME, forceMergeAction))
        );
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest));
    }

    /**
     * Creates a data stream with the given name, number of primary shards and replicas, and attaches the singleton ILM policy to it.
     */
    private void createDataStream(String dataStream, int primaries, int replicas) {
        final var composableIndexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(Template.builder().settings(indexSettings(primaries, replicas).put(LifecycleSettings.LIFECYCLE_NAME, policy)))
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("template-" + dataStream).indexTemplate(composableIndexTemplate)
            )
        );
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest));
    }

    /**
     * Creates an index with the given name, number of primary shards and replicas, and attaches the singleton ILM policy to it.
     * Also creates an alias with the given name that is the write index.
     */
    private void createIndexWithAlias(String index, String alias, int primaries, int replicas) {
        assertAcked(
            prepareCreate(
                index,
                indexSettings(primaries, replicas).put(LifecycleSettings.LIFECYCLE_NAME, policy)
                    .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
            ).addAlias(new Alias(alias).writeIndex(true))
        );
    }

    /**
     * Indexes two documents into the target index, refreshing and flushing in between, to try to get multiple segments. We can't
     * always guarantee multiple segments, as the shard may merge them already. Having multiple segments increases the value of the test
     * as it ensures the force merge action actually has something to do.
     * Note, if the index has more than one (primary) shard, the two documents are likely to end up in different shards, reducing the chance
     * of multiple segments per shard even more.
     */
    private void indexAndRollover(String targetName) {
        for (int i = 0; i < 2; i++) {
            prepareIndex(targetName).setCreate(true)
                .setSource("@timestamp", "2025-08-23", "field", "value-" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            indicesAdmin().prepareFlush(targetName).setForce(true).get();
        }
        // This will usually create two segments, but we can't reliably assert that there are exactly two segments,
        // as the shard may have merged them already.
    }

    /**
     * Waits until the given index is at the specified step in the specified phase. If phase is null, only action and step are checked.
     */
    private static void awaitStep(String indexName, @Nullable String phase, String action, String step) {
        awaitClusterState(state -> {
            final var indexMetadata = state.metadata().getProject(ProjectId.DEFAULT).index(indexName);
            if (indexMetadata == null) {
                return false;
            }
            final var executionState = indexMetadata.getLifecycleExecutionState();
            // We wait until the index at least has an execution state, and isn't in the "new" phase anymore.
            if (executionState == null || "new".equals(executionState.phase())) {
                return false;
            }
            return (phase == null || phase.equals(executionState.phase()))
                && action.equals(executionState.action())
                && step.equals(executionState.step());
        });
    }

    /**
     * Waits until a clone of the given index exists that was created by the force merge action, and returns its name.
     */
    private String awaitForceMergedIndexName(String sourceIndexName) {
        SetOnce<String> forceMergedIndexName = new SetOnce<>();
        awaitClusterState(state -> {
            final var foundIndex = state.metadata()
                .getProject(ProjectId.DEFAULT)
                .indices()
                .keySet()
                .stream()
                .filter(i -> i.startsWith(ForceMergeAction.FORCE_MERGED_INDEX_PREFIX) && i.endsWith(sourceIndexName))
                .findFirst();
            if (foundIndex.isEmpty()) {
                return false;
            }
            forceMergedIndexName.set(foundIndex.get());
            return true;
        });
        logger.info("--> found force merge index [{}]", forceMergedIndexName.get());
        return forceMergedIndexName.get();
    }

    /**
     * Asserts that the force merge action on a data stream has completed as expected.
     * Checks that the force merged index has the correct codec, number of primaries and replicas,
     * and that the data stream's backing indices are as expected depending on the number of replicas.
     */
    private void assertDataStreamForceMergeComplete(
        String forceMergedIndex,
        String dataStream,
        boolean withCodec,
        int expectedPrimaries,
        int expectedReplicas
    ) {
        String expectedCodec = withCodec ? CodecService.BEST_COMPRESSION_CODEC : null;
        String sourceIndexName = assertForceMergeComplete(forceMergedIndex, expectedCodec, expectedPrimaries, expectedReplicas);

        // Ensure the data stream has the expected backing indices.
        List<String> dataStreamBackingIndexNames = getDataStreamBackingIndexNames(dataStream);
        if (expectedReplicas > 0) {
            // With replicas we clone the index, so the source index should be gone.
            assertThat(dataStreamBackingIndexNames, hasItem(forceMergedIndex));
            assertThat(dataStreamBackingIndexNames, not(hasItem(sourceIndexName)));
        } else {
            // With 0 replicas we force merge in place, so the source index is the force merged index
            assertThat(dataStreamBackingIndexNames, hasItem(sourceIndexName));
        }
    }

    /**
     * Asserts that the force merge action on an index with an alias has completed as expected.
     * Checks that the force merged index has the correct codec, number of primaries and replicas,
     * and that the required aliases were created and pointing to the correct indices.
     */
    private void assertAliasForceMergeComplete(
        String forceMergedIndex,
        String aliasName,
        boolean withCodec,
        int expectedPrimaries,
        int expectedReplicas
    ) {
        // ESIntegTestCase creates a random_index_template which sets the index.codec setting to the lucene_default codec.
        String expectedCodec = withCodec ? CodecService.BEST_COMPRESSION_CODEC : CodecService.LUCENE_DEFAULT_CODEC;
        String sourceIndexName = assertForceMergeComplete(forceMergedIndex, expectedCodec, expectedPrimaries, expectedReplicas);

        // Ensure that a new alias was created that points to the force merged index if the index was cloned.
        GetAliasesResponse getAliasesResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT).get();
        assertEquals("Expected two index keys in aliases output, but got " + getAliasesResponse, 2, getAliasesResponse.getAliases().size());
        getAliasesResponse.getAliases().forEach((index, aliases) -> {
            List<String> aliasNames = aliases.stream().map(AliasMetadata::alias).toList();
            if (index.equals(forceMergedIndex)) {
                if (expectedReplicas > 0) {
                    // With replicas we clone the index, so we should have the original alias and an alias with the source index name.
                    assertThat(aliasNames, containsInAnyOrder(aliasName, sourceIndexName));
                } else {
                    // With 0 replicas we force merge in place, so the source index is the force merged index.
                    assertThat(aliasNames, containsInAnyOrder(aliasName));
                }
            } else {
                // The current write index of the alias.
                assertThat(aliasNames, containsInAnyOrder(aliasName));
                var alias = aliases.get(0);
                assertNotEquals(sourceIndexName, index);
                assertTrue(alias.writeIndex());
            }
        });
    }

    /**
     * Asserts that the force merged index has completed the ILM policy, has the expected codec, number of primaries and replicas,
     * and that the source index is deleted if the force merged index was cloned.
     * Returns the name of the source index. If the force merge was done in place (0 replicas), this is the same as the force merged index.
     */
    private String assertForceMergeComplete(String forceMergedIndex, String expectedCodec, int expectedPrimaries, int expectedReplicas) {
        // Wait for the force merged index to have completed the ILM policy.
        awaitStep(forceMergedIndex, null, PhaseCompleteStep.NAME, PhaseCompleteStep.NAME);
        logger.info("--> force merge complete on index [{}]", forceMergedIndex);

        getShardSegments(forceMergedIndex).forEach(
            s -> assertTrue("Expected max 1 segment but got " + s.getSegments(), s.getSegments().size() <= 1)
        );

        Settings indexSettings = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, forceMergedIndex)
            .get()
            .getIndexToSettings()
            .get(forceMergedIndex);
        // Ensure the index has the expected number of shards and replicas.
        int actualPrimaries = indexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1);
        int actualReplicas = indexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1);
        assertEquals("Unexpected number of primary shards", expectedPrimaries, actualPrimaries);
        assertEquals("Unexpected number of replica shards", expectedReplicas, actualReplicas);
        // Ensure the index is not read-only
        assertNull(indexSettings.get(IndexMetadata.APIBlock.WRITE.settingName()));
        // Ensure the index has the expected codec setting
        String actualCodec = indexSettings.get(EngineConfig.INDEX_CODEC_SETTING.getKey());
        assertEquals(expectedCodec, actualCodec);
        // Ensure ILM does not skip this index
        assertNull(indexSettings.get(LifecycleSettings.LIFECYCLE_SKIP));

        // We only clone the index if there are more than 0 replicas - with 0 replicas we force merge in place.
        if (expectedReplicas > 0) {
            // Ensure the source index is deleted - we need to wait for this, as the ILM execution state is copied over to the cloned index
            // before the source index is deleted.
            String sourceIndexName = indexSettings.get(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY);
            assertNotNull(sourceIndexName);
            awaitIndexNotExists(sourceIndexName);
            return sourceIndexName;
        }

        return forceMergedIndex;
    }
}
