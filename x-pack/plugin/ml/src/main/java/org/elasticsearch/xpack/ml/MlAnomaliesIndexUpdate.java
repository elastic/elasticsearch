/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Rollover the various .ml-anomalies result indices updating the read and write aliases.
 * Also detects and heals indices that were rolled over with an incorrect dynamic job_id
 * mapping (the ".reindexed-v7-ml-anomalies-*" bug introduced in ES 8.18/8.19).
 */
public class MlAnomaliesIndexUpdate implements MlAutoUpdateService.UpdateAction {

    private static final Logger logger = LogManager.getLogger(MlAnomaliesIndexUpdate.class);

    /**
     * Kill-switch: set to {@code false} to disable the reindexed-v7 heal step without affecting
     * the normal rollover loop. Dynamically updatable.
     */
    public static final Setting<Boolean> HEAL_REINDEXED_V7_ENABLED = Setting.boolSetting(
        "xpack.ml.anomalies.heal_reindexed_v7.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** Prefix that identifies a bad "reindexed-v7" anomalies index. */
    static final String REINDEXED_V7_PREFIX = ".reindexed-v7-";

    /** Index pattern used to resolve all .reindexed-v7-ml-anomalies-* candidates. */
    static final String REINDEXED_V7_PATTERN = REINDEXED_V7_PREFIX + "ml-anomalies-*";

    /**
     * Timeout for each synchronous alias-update call during the heal step.
     */
    private static final TimeValue HEAL_TIMEOUT = TimeValue.timeValueMinutes(5);

    /** Max collision retries when the derived target index name already exists. */
    private static final int MAX_SUFFIX_RETRIES = 5;

    private final IndexNameExpressionResolver expressionResolver;
    private final OriginSettingClient client;
    private final AnomalyDetectionAuditor auditor;
    private final BooleanSupplier healEnabled;

    public MlAnomaliesIndexUpdate(
        IndexNameExpressionResolver expressionResolver,
        Client client,
        AnomalyDetectionAuditor auditor,
        BooleanSupplier healEnabled
    ) {
        this.expressionResolver = expressionResolver;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.auditor = auditor;
        this.healEnabled = healEnabled;
    }

    @Override
    public boolean isMinTransportVersionSupported(TransportVersion minTransportVersion) {
        // Automatic rollover does not require any new features
        // but wait for all nodes to be upgraded anyway
        return true;
    }

    @Override
    public boolean isAbleToRun(ClusterState latestState) {
        // Find the .ml-anomalies-shared and all custom results indices
        String[] indices = expressionResolver.concreteIndexNames(
            latestState,
            IndicesOptions.lenientExpandOpenHidden(),
            AnomalyDetectorsIndex.jobResultsIndexPattern()
        );

        for (String index : indices) {
            IndexRoutingTable routingTable = latestState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getName() {
        return "ml_anomalies_index_update";
    }

    /**
     * Executes updates related to ML anomaly indices.
     * - Runs the rollover process for ML anomaly indices.
     * - Attempts to heal any <code>.reindexed-v7-ml-anomalies-*</code> indices if necessary.
     * If either task fails, an aggregated {@link ElasticsearchStatusException} is thrown
     * containing all suppressed failures.
     *
     * @param latestState The latest {@link ClusterState} to operate against.
     * @throws ElasticsearchStatusException if one or more update steps fail.
     */
    @Override
    public void runUpdate(ClusterState latestState) {
        // roll over legacy ml anomalies indices if necessary
        Exception rolloverFailure = null;
        try {
            runRolloverLoop(latestState);
        } catch (Exception e) {
            rolloverFailure = e;
        }

        // heal reindexed-v7 ml anomalies indices if necessary
        Exception healFailure = null;
        try {
            healReindexedV7Anomalies(latestState);
        } catch (Exception e) {
            healFailure = e;
        }

        if (rolloverFailure != null || healFailure != null) {
            var combined = new ElasticsearchStatusException("One or more ml anomalies index update steps failed.", RestStatus.CONFLICT);
            if (rolloverFailure != null) combined.addSuppressed(rolloverFailure);
            if (healFailure != null) combined.addSuppressed(healFailure);
            throw combined;
        }
    }

    /**
     * Rolls over each outdated or legacy ML anomaly index to ensure all indices are up-to-date and compatible.
     * Only acts on the latest index in each group. Records any failures.
     *
     * @param latestState Cluster state used for resolving anomaly indices.
     */
    private void runRolloverLoop(ClusterState latestState) {
        List<Exception> failures = new ArrayList<>();

        // list all indices starting .ml-anomalies-
        // this includes the shared index and all custom results indices
        String[] indices = expressionResolver.concreteIndexNames(
            latestState,
            IndicesOptions.lenientExpandOpenHidden(),
            AnomalyDetectorsIndex.jobResultsIndexPattern()
        );

        if (indices.length == 0) {
            return;
        }

        var baseIndicesMap = Arrays.stream(indices).collect(Collectors.groupingBy(MlIndexAndAlias::baseIndexName));

        for (String index : indices) {
            boolean isCompatibleIndexVersion = MlIndexAndAlias.indexIsReadWriteCompatibleInV9(
                latestState.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(index).getCreationVersion()
            );

            // Ensure the index name is of a format amenable to simplifying maintenance
            boolean isCompatibleIndexFormat = MlIndexAndAlias.has6DigitSuffix(index);

            if (isCompatibleIndexVersion && isCompatibleIndexFormat) {
                continue;
            }

            // Check if this index has already been rolled over
            String latestIndex = MlIndexAndAlias.latestIndexMatchingBaseName(index, expressionResolver, latestState);

            if (index.equals(latestIndex) == false) {
                logger.debug("index [{}] will not be rolled over as there is a later index [{}]", index, latestIndex);
                continue;
            }

            PlainActionFuture<Boolean> updated = new PlainActionFuture<>();
            rollAndUpdateAliases(latestState, index, baseIndicesMap.get(MlIndexAndAlias.baseIndexName(index)), updated);
            try {
                updated.actionGet();
            } catch (Exception ex) {
                var message = "failed rolling over legacy ml anomalies index [" + index + "]";
                logger.warn(message, ex);
                if (ex instanceof ElasticsearchException elasticsearchException) {
                    failures.add(new ElasticsearchStatusException(message, elasticsearchException.status(), elasticsearchException));
                } else {
                    failures.add(new ElasticsearchStatusException(message, RestStatus.REQUEST_TIMEOUT, ex));
                }

                break;
            }
        }

        if (failures.isEmpty()) {
            logger.info("legacy ml anomalies indices rolled over and aliases updated");
            return;
        }

        var exception = new ElasticsearchStatusException("failed to roll over legacy ml anomalies", RestStatus.CONFLICT);
        failures.forEach(exception::addSuppressed);
        throw exception;
    }

    private void rollAndUpdateAliases(ClusterState clusterState, String index, List<String> baseIndices, ActionListener<Boolean> listener) {
        Tuple<String, String> newIndexNameAndRolloverAlias = MlIndexAndAlias.createRolloverAliasAndNewIndexName(index);
        String rolloverAlias = newIndexNameAndRolloverAlias.v1();
        String newIndexName = newIndexNameAndRolloverAlias.v2();

        IndicesAliasesRequestBuilder aliasRequestBuilder = MlIndexAndAlias.createIndicesAliasesRequestBuilder(client);

        SubscribableListener.<Boolean>newForked(
            l -> { createAliasForRollover(index, rolloverAlias, l.map(AcknowledgedResponse::isAcknowledged)); }
        ).<String>andThen((l, success) -> {
            rollover(rolloverAlias, newIndexName, l);
        }).<Boolean>andThen((l, newIndexNameResponse) -> {
            MlIndexAndAlias.addResultsIndexRolloverAliasActions(aliasRequestBuilder, newIndexNameResponse, clusterState, baseIndices);
            // Delete the new alias created for the rollover action
            aliasRequestBuilder.removeAlias(newIndexNameResponse, rolloverAlias);
            MlIndexAndAlias.updateAliases(aliasRequestBuilder, l);
        }).addListener(listener);
    }

    private void rollover(String alias, @Nullable String newIndexName, ActionListener<String> listener) {
        MlIndexAndAlias.rollover(client, new RolloverRequest(alias, newIndexName), listener);
    }

    private void createAliasForRollover(String indexName, String aliasName, ActionListener<IndicesAliasesResponse> listener) {
        MlIndexAndAlias.createAliasForRollover(client, indexName, aliasName, listener);
    }

    /**
     * Detects any {@code .reindexed-v7-ml-anomalies-*} indices that still carry live
     * {@code .ml-anomalies-*} read/write aliases (i.e., that were created in the buggy
     * template-mismatch window), and moves those aliases to a correctly-mapped target index.
     *
     * The method is a no-op when the kill-switch setting is {@code false} or when no
     * affected indices are found. Failures on individual candidates are collected and
     * re-thrown as a combined {@link ElasticsearchStatusException}.
     *
     * @param state Cluster state used for resolving anomaly indices.
     */
    void healReindexedV7Anomalies(ClusterState state) {
        if (healEnabled.getAsBoolean() == false) {
            logger.debug("Setting [{}] is disabled; heal step skipped", HEAL_REINDEXED_V7_ENABLED.getKey());
            return;
        }

        // Resolve all .reindexed-v7-ml-anomalies-* candidates from cluster state
        String[] candidates = expressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpenHidden(), REINDEXED_V7_PATTERN);

        List<Exception> failures = new ArrayList<>();

        // Memoize the target index per base, so related bad indices share one new target index.

        Map<String, String> targetByBase = new HashMap<>();

        for (String badIndex : candidates) {
            try {
                healOneBadIndex(badIndex, state, targetByBase);
            } catch (Exception e) {
                logger.warn("failed to heal reindexed-v7 ml anomalies index [{}]", badIndex, e);
                String msg = Strings.format("failed to heal reindexed-v7 ml anomalies index [%s]", badIndex);
                if (e instanceof ElasticsearchException ee) {
                    failures.add(new ElasticsearchStatusException(msg, ee.status(), ee));
                } else {
                    failures.add(new ElasticsearchStatusException(msg, RestStatus.INTERNAL_SERVER_ERROR, e));
                }
            }
        }

        if (failures.isEmpty() == false) {
            var combined = new ElasticsearchStatusException("one or more reindexed-v7 ml anomalies heal steps failed", RestStatus.CONFLICT);
            failures.forEach(combined::addSuppressed);
            throw combined;
        }
    }

    /**
     * If necessary, create a new index with the name that does not clash with any existing index and move
     * the aliases from the bad index to the new index.
     *
     * @param badIndex     The index to heal.
     * @param state        The cluster state snapshot for this heal run.
     * @param targetByBase Maps target base name to created target index for this heal run, so related indices use the same new target index.

     */
    private void healOneBadIndex(String badIndex, ClusterState state, Map<String, String> targetByBase) {
        // Determine if the mapping is broken (absent or non-keyword job_id)
        if (hasCorrectJobIdMapping(badIndex, state)) {
            return;
        }

        // Skip if there are no live ml-anomalies aliases still on this bad index
        Map<String, List<AliasMetadata>> aliasesMap = state.metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .findAllAliases(new String[] { badIndex });
        List<AliasMetadata> liveAliases = aliasesMap.getOrDefault(badIndex, List.of())
            .stream()
            .filter(am -> MlIndexAndAlias.isAnomaliesReadAlias(am.alias()) || MlIndexAndAlias.isAnomaliesWriteAlias(am.alias()))
            .toList();
        if (liveAliases.isEmpty()) {
            return;
        }

        // Extract the distinct job ids from the aliases
        Set<String> jobIds = new LinkedHashSet<>();
        for (AliasMetadata am : liveAliases) {
            AnomalyDetectorsIndex.jobIdFromAlias(am.alias()).ifPresent(jobIds::add);
        }

        // Derive target base name (strip .reindexed-v7- prefix and 6-digit suffix)
        String strippedName = "." + badIndex.substring(REINDEXED_V7_PREFIX.length()); // e.g. ".ml-anomalies-shared-000001"
        String targetBase = MlIndexAndAlias.baseIndexName(strippedName);               // e.g. ".ml-anomalies-shared"

        String targetIndex = targetByBase.computeIfAbsent(targetBase, base -> resolveOrCreateTargetIndex(base, state));
        moveAliasesToTarget(badIndex, targetIndex, jobIds);
        emitAdvisoryNotifications(badIndex, targetIndex);

        logger.warn(
            "The ML anomalies index [{}] was missing required keyword mapping for [job_id]. "
                + "Aliases for jobs [{}] have been moved to a compatible anomalies index [{}].",
            badIndex,
            jobIds,
            targetIndex
        );

    }

    /**
     * Returns {@code true} iff the {@code job_id} field in {@code indexName}'s mapping
     * is typed as {@code keyword} (i.e. the ML index template was applied correctly).
     */
    private boolean hasCorrectJobIdMapping(String indexName, ClusterState state) {
        var indexMetadata = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(indexName);
        return MlIndexAndAlias.hasFieldTypedAs(indexMetadata, Job.ID.getPreferredName(), "keyword");
    }

    /**
     * Finds the most-recent existing target index that is v9-compatible and has
     * a keyword job_id mapping (i.e., the template was applied). Falls back to
     * creating a new index with the next free 6-digit suffix.
     */
    private String resolveOrCreateTargetIndex(String targetBase, ClusterState state) {
        String[] existingFamily = MlIndexAndAlias.strictFamilyOf(targetBase, expressionResolver, state);
        if (existingFamily.length > 0) {
            String latest = MlIndexAndAlias.latestIndex(existingFamily);
            var latestMeta = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(latest);
            if (latestMeta != null
                && MlIndexAndAlias.indexIsReadWriteCompatibleInV9(latestMeta.getCreationVersion())
                && hasCorrectJobIdMapping(latest, state)) {
                logger.debug("[ml_anomalies_reindexed_v7_heal] reusing existing target index [{}] for base [{}]", latest, targetBase);
                return latest;
            }
        }

        // Need to create a new index; find the next free suffix
        return createNewTargetIndex(targetBase, existingFamily);
    }

    /**
     * Creates a new target index under {@code targetBase} with the next free 6-digit suffix.
     * Retries up to {@link #MAX_SUFFIX_RETRIES} times on {@link ResourceAlreadyExistsException}.
     * <p>
     * {@code existingFamily} comes from the cluster-state at the start of {@link #runUpdate}.
     * If another node creates a conflicting index before this method runs, we increment
     * the suffix and retry. {@code MAX_SUFFIX_RETRIES = 5} covers all likely cases,
     * since (a) {@code targetByBase} allows just one create per base per run, and
     * (b) outside creation of {@code .ml-anomalies-*} indices is extremely rare.
     * If all retries fail, the heal is skipped for this candidate; no data is lost,
     * and the next {@link #runUpdate} will work once state is up to date.

     */
    private String createNewTargetIndex(String targetBase, String[] existingFamily) {
        int firstSuffix = MlIndexAndAlias.nextSuffix(existingFamily);

        for (int attempt = 0; attempt < MAX_SUFFIX_RETRIES; attempt++) {
            String targetName = targetBase + "-" + Strings.format("%06d", firstSuffix + attempt);

            PlainActionFuture<CreateIndexResponse> createFuture = new PlainActionFuture<>();
            CreateIndexRequest request = new CreateIndexRequest(targetName);
            request.waitForActiveShards(ActiveShardCount.DEFAULT);

            executeAsyncWithOrigin(client, ML_ORIGIN, TransportCreateIndexAction.TYPE, request, createFuture);

            try {
                createFuture.actionGet(HEAL_TIMEOUT);
            } catch (ResourceAlreadyExistsException e) {
                logger.debug("[ml_anomalies_reindexed_v7_heal] target [{}] already exists; trying next suffix", targetName);
                continue;
            }

            // Wait for yellow before returning
            waitForYellow(targetName);
            logger.info("[ml_anomalies_reindexed_v7_heal] created target index [{}]", targetName);
            return targetName;
        }

        throw new ElasticsearchStatusException(
            "unable to create target index for base [" + targetBase + "] after " + MAX_SUFFIX_RETRIES + " attempts",
            RestStatus.CONFLICT
        );
    }

    private void waitForYellow(String indexName) {
        PlainActionFuture<ClusterHealthResponse> healthFuture = new PlainActionFuture<>();
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(HEAL_TIMEOUT, indexName).waitForYellowStatus()
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true);

        executeAsyncWithOrigin(client, ML_ORIGIN, TransportClusterHealthAction.TYPE, healthRequest, healthFuture);
        try {
            healthFuture.actionGet(HEAL_TIMEOUT);
        } catch (Exception e) {
            // A timed-out health request is not fatal; the alias update will still work.
            logger.debug(
                "[ml_anomalies_reindexed_v7_heal] cluster health wait for [{}] failed or timed out: {}",
                indexName,
                e.getMessage()
            );
        }
    }

    /**
     * Builds a single atomic {@link IndicesAliasesRequest} that removes all of the job's
     * read and write aliases from {@code badIndex} (with {@code mustExist=false} for
     * idempotency) and adds them to {@code targetIndex}.
     */
    private void moveAliasesToTarget(String badIndex, String targetIndex, Set<String> jobIds) {
        IndicesAliasesRequestBuilder req = MlIndexAndAlias.createIndicesAliasesRequestBuilder(client);

        for (String jobId : jobIds) {
            String writeAlias = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
            String readAlias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

            // Remove from bad index — mustExist(false) tolerates already-moved aliases
            req.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(badIndex).alias(writeAlias).mustExist(false));
            req.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(badIndex).alias(readAlias).mustExist(false));

            // Add to target index
            req.addAliasAction(
                IndicesAliasesRequest.AliasActions.add().index(targetIndex).alias(writeAlias).isHidden(true).writeIndex(true)
            );
            req.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                    .index(targetIndex)
                    .alias(readAlias)
                    .isHidden(true)
                    .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
            );
        }

        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        MlIndexAndAlias.updateAliases(req, future);
        try {
            future.actionGet(HEAL_TIMEOUT);
        } catch (Exception e) {
            throw new ElasticsearchStatusException(
                "failed to move aliases from [" + badIndex + "] to [" + targetIndex + "]",
                e instanceof ElasticsearchException ee ? ee.status() : RestStatus.REQUEST_TIMEOUT,
                e
            );
        }
    }

    /**
     * Emits one actionable warning notification per healed bad index.
     */
    private void emitAdvisoryNotifications(String badIndex, String targetIndex) {
        String clusterMessage = Strings.format(
            "Anomaly detection historical results are stranded in index [%s] because an Elasticsearch upgrade "
                + "on one of the affected versions (pre-8.18.8, pre-8.19.5, pre-9.0.8, pre-9.1.5, pre-9.2.0) "
                + "produced a broken dynamic job_id mapping. "
                + "New results are now written to [%s] with the correct mappings and will appear in the UI again. "
                + "To recover the historical results, ensure your user has read and write on .ml-anomalies-* "
                + "(or manage_ml / superuser) and run: "
                + "POST _reindex?wait_for_completion=false "
                + "{\"source\":{\"index\":\"%s\"},\"dest\":{\"index\":\"%s\",\"op_type\":\"create\"}}. "
                + "Using op_type:create avoids overwriting documents that were already renormalized in the destination. "
                + "After verifying documents arrived, [%s] may be deleted. "
                + "KB: https://support.elastic.dev/knowledge/view/d699924c",
            badIndex,    // 1 stranded in index
            targetIndex, // 2 new results written to
            badIndex,    // 3 reindex source
            targetIndex, // 4 reindex dest
            badIndex     // 5 may be deleted
        );

        try {
            auditor.warning(AbstractAuditor.All_RESOURCES_ID, clusterMessage);
        } catch (Exception e) {
            logger.warn("Failed to emit cluster-wide audit notification for [{}]", badIndex, e);
        }
    }
}
