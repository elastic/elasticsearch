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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Rollover the various .ml-anomalies result indices updating the read and write aliases.
 * Also detects and heals indices that were rolled over with incorrect mappings from the
 * reindexed-ml-anomalies bug introduced in ES 8.18/8.19 (including
 * {@code .reindexed-v7-ml-anomalies-*} and {@code .reindexed-v8-ml-anomalies-*} lineages).
 */
public class MlAnomaliesIndexUpdate implements MlAutoUpdateService.UpdateAction {

    private static final Logger logger = LogManager.getLogger(MlAnomaliesIndexUpdate.class);

    /**
     * Kill-switch: set to {@code false} to disable the reindexed-anomalies heal step without affecting
     * the normal rollover loop. Dynamically updatable.
     * <p>
     * Setting key retains {@code reindexed_v7} for backwards compatibility; the heal applies to
     * all {@code .reindexed-*-ml-anomalies-*} indices with broken results-index mappings.
     */
    public static final Setting<Boolean> HEAL_REINDEXED_V7_ENABLED = Setting.boolSetting(
        "xpack.ml.anomalies.heal_reindexed_v7.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Index pattern used to resolve all reindexed ML anomalies candidates (e.g.
     * {@code .reindexed-v7-ml-anomalies-*}, {@code .reindexed-v8-ml-anomalies-*}).
     * Broader than the composable template's explicit v7/v8 {@code index_patterns} because
     * runtime index resolution is not subject to template-overlap validation.
     */
    static final String REINDEXED_ANOMALIES_PATTERN = ".reindexed-*-ml-anomalies-*";

    /**
     * {@code anomaly_score_explanation} fields that must be typed as {@code double} in the
     * managed AD results index template. Dynamic mapping may guess {@code float} instead.
     */
    static final List<String> ANOMALY_SCORE_EXPLANATION_DOUBLE_FIELDS = List.of(
        "lower_confidence_bound",
        "typical_value",
        "upper_confidence_bound",
        "by_field_relative_rarity"
    );

    private static final String ANOMALY_SCORE_EXPLANATION_OBJECT = "anomaly_score_explanation";

    /**
     * Strips {@code .reindexed-<version>-} from a concrete index name, leaving {@code ml-anomalies-...}
     * so {@link MlIndexAndAlias#baseIndexName} can derive the canonical results base (e.g. {@code .ml-anomalies-shared}).
     */
    private static final Pattern REINDEXED_ANOMALIES_STRIP = Pattern.compile("^\\.reindexed-[^-]+-(ml-anomalies-.*)$");

    /**
     * Timeout for each synchronous alias-update call during the heal step.
     */
    private static final TimeValue HEAL_TIMEOUT = TimeValue.timeValueMinutes(5);

    /** Max collision retries when the derived target index name already exists. */
    private static final int MAX_SUFFIX_RETRIES = 5;

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver expressionResolver;
    private final OriginSettingClient client;
    private final AnomalyDetectionAuditor auditor;
    private final SystemAuditor systemAuditor;
    private final BooleanSupplier healEnabled;

    public MlAnomaliesIndexUpdate(
        ClusterService clusterService,
        IndexNameExpressionResolver expressionResolver,
        Client client,
        AnomalyDetectionAuditor auditor,
        SystemAuditor systemAuditor,
        BooleanSupplier healEnabled
    ) {
        this.clusterService = clusterService;
        this.expressionResolver = expressionResolver;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.auditor = auditor;
        this.systemAuditor = systemAuditor;
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
     * - Heals any <code>.reindexed-*-ml-anomalies-*</code> indices with broken results mappings if necessary.
     * - Runs the rollover process for ML anomaly indices against a fresh {@link ClusterState} so rollovers see
     *   aliases already moved off reindexed lineages by the heal step.
     * If either task fails, an aggregated {@link ElasticsearchStatusException} is thrown
     * containing all suppressed failures.
     *
     * @param latestState The latest {@link ClusterState} to operate against.
     * @throws ElasticsearchStatusException if one or more update steps fail.
     */
    @Override
    public void runUpdate(ClusterState latestState) {
        Exception healFailure = null;
        try {
            healReindexedAnomalies(latestState);
        } catch (Exception e) {
            healFailure = e;
        }

        Exception rolloverFailure = null;
        try {
            runRolloverLoop(clusterService.state());
        } catch (Exception e) {
            rolloverFailure = e;
        }

        if (rolloverFailure != null || healFailure != null) {
            var combined = new ElasticsearchStatusException("One or more ml anomalies index update steps failed.", RestStatus.CONFLICT);
            if (healFailure != null) combined.addSuppressed(healFailure);
            if (rolloverFailure != null) combined.addSuppressed(rolloverFailure);
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
     * Detects any {@code .reindexed-*-ml-anomalies-*} indices that still carry live
     * {@code .ml-anomalies-*} read/write aliases (i.e., that were created in the buggy
     * template-mismatch window), and moves those aliases to a correctly-mapped target index.
     *
     * The method is a no-op when the kill-switch setting is {@code false} or when no
     * affected indices are found. Failures on individual candidates are collected and
     * re-thrown as a combined {@link ElasticsearchStatusException}.
     *
     * @param state Cluster state used for resolving anomaly indices.
     */
    void healReindexedAnomalies(ClusterState state) {
        if (healEnabled.getAsBoolean() == false) {
            logger.debug("Setting [{}] is disabled; heal step skipped", HEAL_REINDEXED_V7_ENABLED.getKey());
            return;
        }

        String[] candidates = expressionResolver.concreteIndexNames(
            state,
            IndicesOptions.lenientExpandOpenHidden(),
            REINDEXED_ANOMALIES_PATTERN
        );

        List<Exception> failures = new ArrayList<>();

        // Memoize the target index per base, so related bad indices share one new target index.

        Map<String, String> targetByBase = new HashMap<>();

        for (String badIndex : candidates) {
            try {
                healOneBadIndex(badIndex, state, targetByBase);
            } catch (Exception e) {
                logger.warn("failed to heal reindexed ml anomalies index [{}]", badIndex, e);
                String msg = Strings.format("failed to heal reindexed ml anomalies index [%s]", badIndex);
                if (e instanceof ElasticsearchException ee) {
                    failures.add(new ElasticsearchStatusException(msg, ee.status(), ee));
                } else {
                    failures.add(new ElasticsearchStatusException(msg, RestStatus.INTERNAL_SERVER_ERROR, e));
                }
            }
        }

        if (failures.isEmpty() == false) {
            var combined = new ElasticsearchStatusException("one or more reindexed ml anomalies heal steps failed", RestStatus.CONFLICT);
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
     * @param targetByBase Maps target base name to created target index for this heal run, so related indices use the
     *                     same new target index.
     */
    private void healOneBadIndex(String badIndex, ClusterState state, Map<String, String> targetByBase) {
        // Determine if the mapping is broken (e.g. non-keyword job_id or float anomaly_score_explanation fields)
        if (isIndexMappingHealthy(badIndex, state)) {
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

        Matcher stripMatch = REINDEXED_ANOMALIES_STRIP.matcher(badIndex);
        if (stripMatch.matches() == false) {
            throw new IllegalStateException("unexpected reindexed ml anomalies index name [" + badIndex + "]");
        }
        // e.g. ".ml-anomalies-shared-000001" or ".ml-anomalies-shared" (no 6-digit suffix)
        String strippedName = "." + stripMatch.group(1);
        String targetBase = MlIndexAndAlias.baseIndexName(strippedName); // e.g. ".ml-anomalies-shared"

        String targetIndex = targetByBase.computeIfAbsent(targetBase, base -> resolveOrCreateTargetIndex(base, state));
        moveAliasesToTarget(badIndex, targetIndex, jobIds);
        emitAdvisoryNotifications(badIndex, targetIndex, jobIds);

        logger.warn(
            "The ML anomalies index [{}] had incompatible results-index mappings. "
                + "Aliases for jobs [{}] have been moved to a compatible anomalies index [{}].",
            badIndex,
            jobIds,
            targetIndex
        );

    }

    /**
     * Returns {@code true} iff the index carries the managed AD results mappings required for
     * job open and alias-filtered reads ({@code job_id: keyword} and
     * {@code anomaly_score_explanation.*: double}).
     */
    private boolean isIndexMappingHealthy(String indexName, ClusterState state) {
        return hasCorrectJobIdMapping(indexName, state) && hasCorrectAnomalyScoreExplanationMapping(indexName, state);
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
     * Returns {@code true} iff every {@link #ANOMALY_SCORE_EXPLANATION_DOUBLE_FIELDS} entry in
     * {@code anomaly_score_explanation} is typed as {@code double}.
     */
    private boolean hasCorrectAnomalyScoreExplanationMapping(String indexName, ClusterState state) {
        var indexMetadata = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID).index(indexName);
        for (String fieldName : ANOMALY_SCORE_EXPLANATION_DOUBLE_FIELDS) {
            if (MlIndexAndAlias.hasFieldTypedAs(indexMetadata, List.of(ANOMALY_SCORE_EXPLANATION_OBJECT, fieldName), "double") == false) {
                return false;
            }
        }
        return true;
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
                && isIndexMappingHealthy(latest, state)) {
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
     * Builds a single atomic {@link IndicesAliasesRequest} that strips the job's read+write
     * aliases from every {@code .reindexed-*-ml-anomalies-*} index that currently claims them
     * (not just {@code badIndex}) and adds them to {@code targetIndex}.
     * <p>
     * Cluster-wide strip is required because UA's 8&rarr;9 reindex copies a v7 source's aliases
     * onto the v8 destination (e.g. {@code .reindexed-v8-ml-anomalies-*}). If we only stripped
     * {@code badIndex}, the surviving sibling reindexed lineage would still hold the read+write
     * alias and the next rollover pass would fail validation with
     * {@code "alias [...] has more than one write index"} as soon as it tries to move the write
     * alias onto a newly created rollover index — the heal target already claims it.
     * <p>
     * Scoping the cluster-wide strip to {@code .reindexed-*-ml-anomalies-*} (i.e.
     * {@link #REINDEXED_ANOMALIES_STRIP}) is deliberate: canonical {@code .ml-anomalies-*-NNNNNN}
     * family members keep their read aliases so historical results in older generations remain
     * queryable. Only the post-migration stale reindexed lineages are evacuated.
     * <p>
     * {@code mustExist(false)} keeps every remove idempotent under racing cluster-state updates.
     */
    private void moveAliasesToTarget(String badIndex, String targetIndex, Set<String> jobIds) {
        IndicesAliasesRequestBuilder req = MlIndexAndAlias.createIndicesAliasesRequestBuilder(client);

        // Freshest available state — heal may have already created the target index after the
        // start-of-runUpdate snapshot, and a prior heal iteration may have moved aliases for
        // other jobs onto the same target.
        var project = clusterService.state().metadata().getProject(Metadata.DEFAULT_PROJECT_ID);

        for (String jobId : jobIds) {
            String writeAlias = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
            String readAlias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

            for (String aliasName : List.of(writeAlias, readAlias)) {
                for (Index claimant : project.aliasedIndices(aliasName)) {
                    String claimantName = claimant.getName();
                    if (claimantName.equals(targetIndex)) {
                        // Never strip from the target; we are about to (re-)add the alias here.
                        continue;
                    }
                    if (REINDEXED_ANOMALIES_STRIP.matcher(claimantName).matches() == false) {
                        // Leave canonical family members alone — older .ml-anomalies-*-NNNNNN
                        // generations legitimately hold the read alias for historical results.
                        continue;
                    }
                    req.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(claimantName).alias(aliasName).mustExist(false));
                }
            }

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
     * Emits one actionable warning notification per healed bad index (cluster-wide, via {@link SystemAuditor},
     * so Kibana ML notifications can surface it under job type "system"), plus a per-job warning for each
     * affected job (via {@link AnomalyDetectionAuditor}).
     */
    private void emitAdvisoryNotifications(String badIndex, String targetIndex, Set<String> jobIds) {
        String affectedJobs = String.join(", ", jobIds);
        String clusterMessage = Strings.format(
            "Anomaly detection historical results are stranded in index [%s] because an Elasticsearch upgrade "
                + "on one of the affected versions (pre-8.18.8, pre-8.19.19, pre-9.0.8, pre-9.1.5, pre-9.2.0, "
                + "pre-9.3.8, pre-9.4.4, pre-9.5.1, pre-9.6.0) produced broken ML anomaly results index mappings. "
                + "New results are now written to [%s] with the correct mappings and will appear in the UI again. "
                + "Affected jobs: [%s]. "
                + "To recover the historical results, ensure your user has read and write on .ml-anomalies-* "
                + "(or manage_ml / superuser) and run: "
                + "POST _reindex?wait_for_completion=false "
                + "{\"source\":{\"index\":\"%s\"},\"dest\":{\"index\":\"%s\",\"op_type\":\"create\"}}. "
                + "Using op_type:create avoids overwriting documents that were already renormalized in the destination. "
                + "After verifying documents arrived, [%s] may be deleted. "
                + "Details: https://github.com/elastic/elasticsearch/issues/147686",
            badIndex,
            targetIndex,
            affectedJobs,
            badIndex,
            targetIndex,
            badIndex
        );

        String perJobMessage = Strings.format(
            "Historical anomaly results for this job are stranded in index [%s] due to broken results-index mappings from "
                + "an earlier upgrade. New results are now being written to [%s]. "
                + "See the cluster-wide ML notification (job type: System) for the recovery procedure. "
                + "Details: https://github.com/elastic/elasticsearch/issues/147686",
            badIndex,
            targetIndex
        );

        try {
            systemAuditor.warning(clusterMessage);
        } catch (Exception e) {
            logger.warn("Failed to emit cluster-wide audit notification for [{}]", badIndex, e);
        }

        for (String jobId : jobIds) {
            try {
                auditor.warning(jobId, perJobMessage);
            } catch (Exception e) {
                logger.warn("Failed to emit per-job audit notification for job [{}]", jobId, e);
            }
        }
    }
}
