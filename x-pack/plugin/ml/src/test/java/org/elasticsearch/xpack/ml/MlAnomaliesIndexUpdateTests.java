/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlAnomaliesIndexUpdateTests extends ESTestCase {

    private static final BooleanSupplier HEAL_ENABLED = () -> true;
    private static final BooleanSupplier HEAL_DISABLED = () -> false;

    private static ClusterService mockClusterService(ClusterState state) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static MlAnomaliesIndexUpdate updater(Client client, ClusterState rolloverState) {
        return updater(client, rolloverState, mock(AnomalyDetectionAuditor.class), mock(SystemAuditor.class), HEAL_ENABLED);
    }

    private static MlAnomaliesIndexUpdate updater(
        Client client,
        ClusterState rolloverState,
        AnomalyDetectionAuditor auditor,
        BooleanSupplier healEnabled
    ) {
        return updater(client, rolloverState, auditor, mock(SystemAuditor.class), healEnabled);
    }

    private static MlAnomaliesIndexUpdate updater(
        Client client,
        ClusterState rolloverState,
        AnomalyDetectionAuditor auditor,
        SystemAuditor systemAuditor,
        BooleanSupplier healEnabled
    ) {
        return new MlAnomaliesIndexUpdate(
            mockClusterService(rolloverState),
            TestIndexNameExpressionResolver.newInstance(),
            client,
            auditor,
            systemAuditor,
            healEnabled
        );
    }

    public void testIsAbleToRun_IndicesDoNotExist() {
        RoutingTable.Builder routingTable = RoutingTable.builder();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        ClusterState state = csBuilder.build();
        var u = updater(mock(Client.class), state);
        assertTrue(u.isAbleToRun(state));
    }

    public void testIsAbleToRun_IndicesHaveNoRouting() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(".ml-anomalies-shared-000001");
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(RoutingTable.builder().build()); // no routing table
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        assertFalse(updater(mock(Client.class), cs).isAbleToRun(cs));
    }

    public void testRunUpdate_UpToDateIndices() {
        String indexName = ".ml-anomalies-sharedindex-000001";
        var jobs = List.of("job1", "job2");
        IndexMetadata.Builder indexMetadata = createResultsIndex(indexName, IndexVersion.current(), jobs, null);

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        var client = mock(Client.class);
        updater(client, cs).runUpdate(cs);
        // everything up to date so no network calls expected
        verify(client).settings();
        verify(client).threadPool();
        verify(client).projectResolver();
        verifyNoMoreInteractions(client);
    }

    public void testRunUpdate_LegacyIndex() {
        String indexName = ".ml-anomalies-sharedindex";
        var jobs = List.of("job1", "job2");
        IndexMetadata.Builder indexMetadata = createResultsIndex(indexName, IndexVersions.V_7_17_0, jobs, null);

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        ClusterState cs = csBuilder.build();
        var client = mockClientWithRolloverAndAlias(indexName);
        updater(client, cs).runUpdate(cs);
        verify(client).settings();
        verify(client, times(7)).threadPool();
        verify(client).projectResolver();
        verify(client, times(2)).execute(same(TransportIndicesAliasesAction.TYPE), any(), any()); // create rollover alias and update
        verify(client).execute(same(RolloverAction.INSTANCE), any(), any());
        verifyNoMoreInteractions(client);
    }

    public void testHealReindexedV7_NoOp_WhenMappingAlreadyKeyword() {
        // job_id and anomaly_score_explanation fields are already correct — do NOT touch this index
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        ClusterState cs = clusterStateWithBadIndex(badIndex, IndexVersion.current(), List.of("jobA"), healthyResultsMapping());
        var client = mock(Client.class);
        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        mockThreadPool(client);

        updater(client, cs, auditor, systemAuditor, HEAL_ENABLED).runUpdate(cs);

        verify(client, never()).execute(same(TransportCreateIndexAction.TYPE), any(), any());
        verify(client, never()).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());
        verify(auditor, never()).warning(any(), any());
        verify(systemAuditor, never()).warning(anyString());
    }

    public void testAnomalyScoreExplanationDoubleFieldsMatchResultsTemplate() {
        IndexMetadata indexMetadata = IndexMetadata.builder(".test-template-mapping")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(AnomalyDetectorsIndex.resultsMapping())
            .build();

        for (String fieldName : MlAnomaliesIndexUpdate.ANOMALY_SCORE_EXPLANATION_DOUBLE_FIELDS) {
            assertTrue(
                "results template must declare anomaly_score_explanation." + fieldName + " as double",
                MlIndexAndAlias.hasFieldTypedAs(indexMetadata, List.of("anomaly_score_explanation", fieldName), "double")
            );
        }
    }

    public void testHealReindexedV7_HealWhenJobIdKeywordButAnomalyScoreExplanationFloat() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String targetIndex = ".ml-anomalies-shared-000001";
        ClusterState cs = clusterStateWithBadIndex(
            badIndex,
            IndexVersion.current(),
            List.of("jobA"),
            keywordJobIdWithFloatAnomalyScoreExplanationMapping()
        );

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        var client = mockClientForHeal(targetIndex);

        updater(client, cs, auditor, systemAuditor, HEAL_ENABLED).runUpdate(cs);

        verify(client).execute(same(TransportCreateIndexAction.TYPE), any(), any());
        verify(client).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());
        verify(systemAuditor).warning(anyString());
        verify(auditor).warning(eq("jobA"), anyString());
    }

    public void testHealReindexedV7_DoesNotReuseTargetWithFloatAnomalyScoreExplanation() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String unhealthyTarget = ".ml-anomalies-shared-000001";
        String newTarget = ".ml-anomalies-shared-000002";

        IndexMetadata.Builder unhealthyTargetMeta = IndexMetadata.builder(unhealthyTarget)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_target")
            )
            .putMapping(keywordJobIdWithFloatAnomalyScoreExplanationMapping());

        IndexMetadata.Builder badMeta = IndexMetadata.builder(badIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_bad")
            )
            .putMapping(textJobIdMapping())
            .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.resultsWriteAlias("jobA")).writeIndex(true).isHidden(true).build())
            .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobResultsAliasedName("jobA")).isHidden(true).build());

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(unhealthyTargetMeta).put(badMeta))
            .build();

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        var client = mockClientForHeal(newTarget);

        updater(client, cs, auditor, systemAuditor, HEAL_ENABLED).runUpdate(cs);

        verify(client).execute(same(TransportCreateIndexAction.TYPE), any(), any());
        verify(client).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());
        verify(systemAuditor).warning(anyString());
        verify(auditor).warning(eq("jobA"), anyString());
    }

    public void testHealReindexedV7_NoOp_WhenAliasesAlreadyMoved() {
        // Bad index exists with broken mapping but has NO .ml-anomalies-* aliases left → skip
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        // Build cluster state WITHOUT any alias on the bad index
        IndexMetadata.Builder idxMeta = IndexMetadata.builder(badIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_bad")
            )
            .putMapping(textJobIdMapping());
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder().put(idxMeta)).build();

        var client = mock(Client.class);
        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        mockThreadPool(client);

        updater(client, cs, auditor, systemAuditor, HEAL_ENABLED).runUpdate(cs);

        verify(client, never()).execute(same(TransportCreateIndexAction.TYPE), any(), any());
        verify(auditor, never()).warning(any(), any());
        verify(systemAuditor, never()).warning(anyString());
    }

    public void testRunUpdate_RolloverFailureDoesNotBlockHeal() {
        // The legacy index triggers a rollover failure; the heal should still run.
        String legacyIndex = ".ml-anomalies-sharedindex";
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String targetIndex = ".ml-anomalies-shared-000001";

        IndexMetadata.Builder legacyMeta = IndexMetadata.builder(legacyIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersions.V_7_17_0)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_legacy")
            )
            .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobResultsAliasedName("legacyJob")).isHidden(true).build())
            .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.resultsWriteAlias("legacyJob")).writeIndex(true).isHidden(true).build());

        IndexMetadata.Builder badMeta = IndexMetadata.builder(badIndex)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_bad")
            )
            .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.resultsWriteAlias("healJob")).writeIndex(true).isHidden(true).build())
            .putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobResultsAliasedName("healJob")).isHidden(true).build())
            .putMapping(textJobIdMapping());

        ClusterState cs = ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder().put(legacyMeta).put(badMeta)).build();

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        var client = mockClientWithRolloverFailureAndHeal(targetIndex);

        var updater = updater(client, cs, auditor, systemAuditor, HEAL_ENABLED);
        var ex = expectThrows(ElasticsearchStatusException.class, () -> updater.runUpdate(cs));
        assertEquals(RestStatus.CONFLICT, ex.status());
        // The combined exception should include the rollover failure as a suppressed cause.
        assertTrue(ex.getSuppressed().length > 0);

        // Heal ran despite rollover failure: create + aliases + audit
        verify(client).execute(same(TransportCreateIndexAction.TYPE), any(), any());
        verify(systemAuditor).warning(anyString());
        verify(auditor).warning(eq("healJob"), anyString());
    }

    public void testHealReindexedV7_AuditorFailureDoesNotFailHeal() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        String targetIndex = ".ml-anomalies-shared-000001";
        ClusterState cs = clusterStateWithBadIndex(badIndex, IndexVersion.current(), List.of("jobA"), textJobIdMapping());

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        doThrow(new RuntimeException("notifications index unavailable")).when(auditor).warning(any(), any());
        doThrow(new RuntimeException("notifications index unavailable")).when(systemAuditor).warning(anyString());

        var client = mockClientForHeal(targetIndex);
        var updater = updater(client, cs, auditor, systemAuditor, HEAL_ENABLED);

        // Should NOT throw even though the auditor threw
        updater.runUpdate(cs);

        // Aliases still moved
        verify(client).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());
    }

    public void testHealReindexedV7_HealDisabled() {
        String badIndex = ".reindexed-v7-ml-anomalies-shared-000001";
        ClusterState cs = clusterStateWithBadIndex(badIndex, IndexVersion.current(), List.of("jobA"), textJobIdMapping());

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        var client = mock(Client.class);
        mockThreadPool(client);

        updater(client, cs, auditor, systemAuditor, HEAL_DISABLED).runUpdate(cs);

        verify(client, never()).execute(same(TransportCreateIndexAction.TYPE), any(), any());
        verify(client, never()).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());
        verify(auditor, never()).warning(any(), any());
        verify(systemAuditor, never()).warning(anyString());
    }

    /**
     * Scoping guard for the cluster-wide alias strip in
     * {@code MlAnomaliesIndexUpdate.moveAliasesToTarget}.
     * <p>
     * The cluster-wide strip evacuates ML aliases from every {@code .reindexed-*-ml-anomalies-*}
     * claimant so a subsequent rollover does not collide with the heal target. It must NOT strip
     * the read alias from canonical {@code .ml-anomalies-*-NNNNNN} family members — older
     * generations legitimately keep that alias for historical results.
     * <p>
     * Setup: a v9-version {@code .reindexed-v7-ml-anomalies-custom-foo-000001} with broken
     * {@code job_id:text} mapping plus a non-write claim on the write alias, the heal-reuse
     * target {@code .ml-anomalies-custom-foo-000002} (v9-version, keyword, current write index),
     * and an older canonical {@code .ml-anomalies-custom-foo-000001} holding the read alias only.
     * <p>
     * Expectation: the captured alias-update request removes the read alias from the
     * {@code .reindexed-v7-*} claimant but NOT from {@code .ml-anomalies-custom-foo-000001}.
     */
    public void testHealReindexedV7_DoesNotStripReadAliasFromOlderCanonicalGeneration() {
        String reindexedBad = ".reindexed-v7-ml-anomalies-custom-foo-000001";
        String oldCanonical = ".ml-anomalies-custom-foo-000001";
        String newCanonical = ".ml-anomalies-custom-foo-000002";
        String jobId = "fooJob";
        String readAlias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String writeAlias = AnomalyDetectorsIndex.resultsWriteAlias(jobId);

        IndexMetadata.Builder oldMeta = IndexMetadata.builder(oldCanonical)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_old")
            )
            .putMapping(healthyResultsMapping())
            .putAlias(AliasMetadata.builder(readAlias).isHidden(true).build());

        IndexMetadata.Builder newMeta = IndexMetadata.builder(newCanonical)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_new")
            )
            .putMapping(healthyResultsMapping())
            .putAlias(AliasMetadata.builder(readAlias).isHidden(true).build())
            .putAlias(AliasMetadata.builder(writeAlias).writeIndex(true).isHidden(true).build());

        IndexMetadata.Builder badMeta = IndexMetadata.builder(reindexedBad)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid_bad")
            )
            .putMapping(textJobIdMapping())
            .putAlias(AliasMetadata.builder(readAlias).isHidden(true).build())
            .putAlias(AliasMetadata.builder(writeAlias).writeIndex(false).isHidden(true).build());

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(oldMeta).put(newMeta).put(badMeta))
            .build();

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);
        SystemAuditor systemAuditor = mock(SystemAuditor.class);
        var client = mockClientForHeal(newCanonical);

        updater(client, cs, auditor, systemAuditor, HEAL_ENABLED).runUpdate(cs);

        // Heal must have fired.
        verify(systemAuditor).warning(anyString());

        // Inspect every alias-update request issued by heal.
        ArgumentCaptor<IndicesAliasesRequest> captor = ArgumentCaptor.forClass(IndicesAliasesRequest.class);
        verify(client, atLeastOnce()).execute(same(TransportIndicesAliasesAction.TYPE), captor.capture(), any());

        boolean strippedFromOldCanonical = captor.getAllValues()
            .stream()
            .flatMap(r -> r.getAliasActions().stream())
            .anyMatch(
                a -> a.actionType() == IndicesAliasesRequest.AliasActions.Type.REMOVE
                    && a.indices().length > 0
                    && a.indices()[0].equals(oldCanonical)
                    && a.aliases().length > 0
                    && a.aliases()[0].equals(readAlias)
            );
        assertFalse("read alias must NOT be removed from older canonical generation", strippedFromOldCanonical);

        boolean strippedFromReindexed = captor.getAllValues()
            .stream()
            .flatMap(r -> r.getAliasActions().stream())
            .anyMatch(
                a -> a.actionType() == IndicesAliasesRequest.AliasActions.Type.REMOVE
                    && a.indices().length > 0
                    && a.indices()[0].equals(reindexedBad)
                    && a.aliases().length > 0
                    && a.aliases()[0].equals(readAlias)
            );
        assertTrue("read alias must be removed from .reindexed-v7-* claimant", strippedFromReindexed);
    }

    private record AliasActionMatcher(String aliasName, String index, IndicesAliasesRequest.AliasActions.Type actionType) {
        boolean matches(IndicesAliasesRequest.AliasActions aliasAction) {
            return aliasAction.actionType() == actionType
                && aliasAction.aliases()[0].equals(aliasName)
                && aliasAction.indices()[0].equals(index);
        }
    }

    /**
     * Creates a cluster-state containing a single ML results index (shared or bad) with
     * the given jobs' aliases and optional mappingSource.
     */
    private ClusterState clusterStateWithBadIndex(String indexName, IndexVersion version, List<String> jobs, String mappingSource) {
        IndexMetadata.Builder indexMetadata = createResultsIndex(indexName, version, jobs, mappingSource);
        return ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder().put(indexMetadata)).build();
    }

    /** Builds an IndexMetadata.Builder with job aliases and optional mapping. */
    private IndexMetadata.Builder createResultsIndex(String indexName, IndexVersion indexVersion, List<String> jobs, String mappingSource) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );

        for (var jobId : jobs) {
            indexMetadata.putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).isHidden(true).build());
            indexMetadata.putAlias(
                AliasMetadata.builder(AnomalyDetectorsIndex.resultsWriteAlias(jobId)).writeIndex(true).isHidden(true).build()
            );
        }

        if (mappingSource != null) {
            indexMetadata.putMapping(mappingSource);
        }

        return indexMetadata;
    }

    /** Mapping JSON where job_id is text (the broken dynamic-mapping case). */
    private static String textJobIdMapping() {
        return "{\"properties\":{\"job_id\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}}}}";
    }

    /** Mapping JSON where job_id is keyword and anomaly_score_explanation doubles are correct. */
    private static String healthyResultsMapping() {
        return """
            {"properties":{"job_id":{"type":"keyword"},"anomaly_score_explanation":{"properties":{\
            "lower_confidence_bound":{"type":"double"},\
            "typical_value":{"type":"double"},\
            "upper_confidence_bound":{"type":"double"},\
            "by_field_relative_rarity":{"type":"double"}}}}}""";
    }

    /** Mapping JSON where job_id is keyword but by_field_relative_rarity was dynamically mapped as float. */
    private static String keywordJobIdWithFloatAnomalyScoreExplanationMapping() {
        return """
            {"properties":{"job_id":{"type":"keyword"},"anomaly_score_explanation":{"properties":{\
            "lower_confidence_bound":{"type":"double"},\
            "typical_value":{"type":"double"},\
            "upper_confidence_bound":{"type":"double"},\
            "by_field_relative_rarity":{"type":"float"}}}}}""";
    }

    /** Mapping JSON where job_id is keyword (healthy) without anomaly_score_explanation fields. */
    private static String keywordJobIdMapping() {
        return "{\"properties\":{\"job_id\":{\"type\":\"keyword\"}}}";
    }

    /** Mocks threadPool() on a client (needed for OriginSettingClient internals). */
    private static void mockThreadPool(Client client) {
        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);
    }

    /**
     * Returns a client mock that succeeds for all heal operations:
     * create-index, cluster-health, and alias update.
     */
    @SuppressWarnings("unchecked")
    static Client mockClientForHeal(String targetIndexName) {
        var client = mock(Client.class);
        mockThreadPool(client);

        // create-index
        doAnswer(inv -> {
            ActionListener<CreateIndexResponse> l = inv.getArgument(2);
            l.onResponse(new CreateIndexResponse(true, true, targetIndexName));
            return null;
        }).when(client).execute(same(TransportCreateIndexAction.TYPE), any(), any());

        // cluster health
        doAnswer(inv -> {
            ActionListener<ClusterHealthResponse> l = inv.getArgument(2);
            l.onResponse(mock(ClusterHealthResponse.class));
            return null;
        }).when(client).execute(same(TransportClusterHealthAction.TYPE), any(), any());

        // alias update
        mockAliasResponse(client);

        return client;
    }

    /** Stubs the alias execute call to respond with success. */
    @SuppressWarnings("unchecked")
    private static void mockAliasResponse(Client client) {
        doAnswer(inv -> {
            ActionListener<IndicesAliasesResponse> l = inv.getArgument(2);
            l.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
            return null;
        }).when(client).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());
    }

    /**
     * A client mock where the rollover action FAILS (simulates a transient rollover error) but
     * heal actions (create + health + alias) succeed.
     */
    @SuppressWarnings("unchecked")
    private static Client mockClientWithRolloverFailureAndHeal(String targetIndexName) {
        var client = mock(Client.class);
        mockThreadPool(client);

        // Rollover fails
        doAnswer(inv -> {
            ActionListener<?> l = inv.getArgument(2);
            l.onFailure(new RuntimeException("rollover failed intentionally"));
            return null;
        }).when(client).execute(same(RolloverAction.INSTANCE), any(RolloverRequest.class), any());

        // Alias for rollover setup also needed
        doAnswer(inv -> {
            ActionListener<IndicesAliasesResponse> l = inv.getArgument(2);
            l.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);
            return null;
        }).when(client).execute(same(TransportIndicesAliasesAction.TYPE), any(IndicesAliasesRequest.class), any());

        // Heal: create-index
        doAnswer(inv -> {
            ActionListener<CreateIndexResponse> l = inv.getArgument(2);
            l.onResponse(new CreateIndexResponse(true, true, targetIndexName));
            return null;
        }).when(client).execute(same(TransportCreateIndexAction.TYPE), any(), any());

        // Heal: cluster-health
        doAnswer(inv -> {
            ActionListener<ClusterHealthResponse> l = inv.getArgument(2);
            l.onResponse(mock(ClusterHealthResponse.class));
            return null;
        }).when(client).execute(same(TransportClusterHealthAction.TYPE), any(), any());

        return client;
    }

    @SuppressWarnings("unchecked")
    static Client mockClientWithRolloverAndAlias(String indexName) {
        var client = mock(Client.class);

        var aliasRequestCount = new AtomicInteger(0);

        doAnswer(invocationOnMock -> {
            ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(new RolloverResponse(indexName, indexName + "-new", Map.of(), false, true, true, true, true));
            return null;
        }).when(client).execute(same(RolloverAction.INSTANCE), any(RolloverRequest.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) invocationOnMock
                .getArguments()[2];
            var request = (IndicesAliasesRequest) invocationOnMock.getArguments()[1];
            // Check the rollover alias is created and deleted
            if (aliasRequestCount.getAndIncrement() == 0) {
                var addAliasAction = new AliasActionMatcher(
                    indexName + ".rollover_alias",
                    indexName,
                    IndicesAliasesRequest.AliasActions.Type.ADD
                );
                assertEquals(1L, request.getAliasActions().stream().filter(addAliasAction::matches).count());
            } else {
                var removeAliasAction = new AliasActionMatcher(
                    indexName + ".rollover_alias",
                    indexName + "-new",
                    IndicesAliasesRequest.AliasActions.Type.REMOVE
                );
                assertEquals(1L, request.getAliasActions().stream().filter(removeAliasAction::matches).count());
            }

            actionListener.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);

            return null;
        }).when(client).execute(same(TransportIndicesAliasesAction.TYPE), any(IndicesAliasesRequest.class), any(ActionListener.class));

        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        return client;
    }
}
