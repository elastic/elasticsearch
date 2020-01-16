/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction.Request;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction.Response;
import org.elasticsearch.xpack.ilm.IndexLifecycleTransition;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class is responsible for bootstrapping {@link IndexLifecycleMetadata} into the cluster-state, as well
 * as adding the desired new policy to be inserted.
 */
public class TransportPutLifecycleAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutLifecycleAction.class);
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;

    @Inject
    public TransportPutLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       NamedXContentRegistry namedXContentRegistry, Client client) {
        super(PutLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
            indexNameExpressionResolver);
        this.xContentRegistry = namedXContentRegistry;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response read(StreamInput in) throws IOException {
        return new Response(in);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
            .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
        clusterService.submitStateUpdateTask("put-lifecycle-" + request.getPolicy().getName(),
                new AckedClusterStateUpdateTask<Response>(request, listener) {
                    @Override
                    protected Response newResponse(boolean acknowledged) {
                        return new Response(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        ClusterState.Builder stateBuilder = ClusterState.builder(currentState);
                        IndexLifecycleMetadata currentMetadata = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);
                        if (currentMetadata == null) { // first time using index-lifecycle feature, bootstrap metadata
                            currentMetadata = IndexLifecycleMetadata.EMPTY;
                        }
                        LifecyclePolicyMetadata existingPolicyMetadata = currentMetadata.getPolicyMetadatas()
                            .get(request.getPolicy().getName());
                        long nextVersion = (existingPolicyMetadata == null) ? 1L : existingPolicyMetadata.getVersion() + 1L;
                        SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
                        LifecyclePolicyMetadata lifecyclePolicyMetadata = new LifecyclePolicyMetadata(request.getPolicy(), filteredHeaders,
                            nextVersion, Instant.now().toEpochMilli());
                        LifecyclePolicyMetadata oldPolicy = newPolicies.put(lifecyclePolicyMetadata.getName(), lifecyclePolicyMetadata);
                        if (oldPolicy == null) {
                            logger.info("adding index lifecycle policy [{}]", request.getPolicy().getName());
                        } else {
                            logger.info("updating index lifecycle policy [{}]", request.getPolicy().getName());
                        }
                        IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentMetadata.getOperationMode());
                        stateBuilder.metaData(MetaData.builder(currentState.getMetaData())
                                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata).build());
                        ClusterState nonRefreshedState = stateBuilder.build();
                        if (oldPolicy == null) {
                            return nonRefreshedState;
                        } else {
                            try {
                                return updateIndicesForPolicy(nonRefreshedState, xContentRegistry, client,
                                    oldPolicy.getPolicy(), lifecyclePolicyMetadata);
                            } catch (Exception e) {
                                logger.warn(new ParameterizedMessage("unable to refresh indices phase JSON for updated policy [{}]",
                                    oldPolicy.getName()), e);
                                // Revert to the non-refreshed state
                                return nonRefreshedState;
                            }
                        }
                    }
                });
    }

    /**
     * Ensure that we have the minimum amount of metadata necessary to check for cache phase
     * refresh. This includes:
     * - An execution state
     * - Existing phase definition JSON
     * - A current step key
     * - A current phase in the step key
     * - Not currently in the ERROR step
     */
    static boolean eligibleToCheckForRefresh(final IndexMetaData metaData) {
        LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(metaData);
        if (executionState == null || executionState.getPhaseDefinition() == null) {
            return false;
        }

        Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(executionState);
        if (currentStepKey == null || currentStepKey.getPhase() == null) {
            return false;
        }

        return ErrorStep.NAME.equals(currentStepKey.getName()) == false;
    }

    /**
     * Parse the {@code phaseDef} phase definition to get the stepkeys for the given phase.
     * If there is an error parsing or if the phase definition is missing the required
     * information, returns null.
     */
    @Nullable
    static Set<Step.StepKey> readStepKeys(final NamedXContentRegistry xContentRegistry, final Client client,
                                          final String phaseDef, final String currentPhase) {
        final PhaseExecutionInfo phaseExecutionInfo;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, phaseDef)) {
            phaseExecutionInfo = PhaseExecutionInfo.parse(parser, currentPhase);
        } catch (Exception e) {
            logger.trace(new ParameterizedMessage("exception reading step keys checking for refreshability, phase definition: {}",
                phaseDef), e);
            return null;
        }

        if (phaseExecutionInfo == null || phaseExecutionInfo.getPhase() == null) {
            return null;
        }

        return phaseExecutionInfo.getPhase().getActions().values().stream()
            .flatMap(a -> a.toSteps(client, phaseExecutionInfo.getPhase().getName(), null).stream())
            .map(Step::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns 'true' if the index's cached phase JSON can be safely reread, 'false' otherwise.
     */
    static boolean isIndexPhaseDefinitionUpdatable(final NamedXContentRegistry xContentRegistry, final Client client,
                                                   final IndexMetaData metaData, final LifecyclePolicy newPolicy) {
        final String index = metaData.getIndex().getName();
        if (eligibleToCheckForRefresh(metaData) == false) {
            logger.debug("[{}] does not contain enough information to check for eligibility of refreshing phase", index);
            return false;
        }
        final String policyId = newPolicy.getName();

        final LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(metaData);
        final Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(executionState);
        final String currentPhase = currentStepKey.getPhase();

        final Set<Step.StepKey> newStepKeys = newPolicy.toSteps(client).stream()
            .map(Step::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));

        if (newStepKeys.contains(currentStepKey) == false) {
            // The index is on a step that doesn't exist in the new policy, we
            // can't safely re-read the JSON
            logger.debug("[{}] updated policy [{}] does not contain the current step key [{}], so the policy phase will not be refreshed",
                index, policyId, currentStepKey);
            return false;
        }

        final String phaseDef = executionState.getPhaseDefinition();
        final Set<Step.StepKey> oldStepKeys = readStepKeys(xContentRegistry, client, phaseDef, currentPhase);
        if (oldStepKeys == null) {
            logger.debug("[{}] unable to parse phase definition for cached policy [{}], policy phase will not be refreshed",
                index, policyId);
            return false;
        }

        final Set<Step.StepKey> oldPhaseStepKeys = oldStepKeys.stream()
            .filter(sk -> currentPhase.equals(sk.getPhase()))
            .collect(Collectors.toCollection(LinkedHashSet::new));

        final PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policyId, newPolicy.getPhases().get(currentPhase), 1L, 1L);
        final String peiJson = Strings.toString(phaseExecutionInfo);

        final Set<Step.StepKey> newPhaseStepKeys = readStepKeys(xContentRegistry, client, peiJson, currentPhase);
        if (newPhaseStepKeys == null) {
            logger.debug(new ParameterizedMessage("[{}] unable to parse phase definition for policy [{}] " +
                "to determine if it could be refreshed", index, policyId));
            return false;
        }

        if (newPhaseStepKeys.equals(oldPhaseStepKeys)) {
            // The new and old phase have the same stepkeys for this current phase, so we can
            // refresh the definition because we know it won't change the execution flow.
            logger.debug("[{}] updated policy [{}] contains the same phase step keys and can be refreshed", index, policyId);
            return true;
        } else {
            logger.debug("[{}] updated policy [{}] has different phase step keys and will NOT refresh phase " +
                    "definition as it differs too greatly. old: {}, new: {}",
                index, policyId, oldPhaseStepKeys, newPhaseStepKeys);
            return false;
        }
    }

    /**
     * Rereads the phase JSON for the given index, returning a new cluster state.
     */
    static ClusterState refreshPhaseDefinition(final ClusterState state, final String index, final LifecyclePolicyMetadata updatedPolicy) {
        final IndexMetaData idxMeta = state.metaData().index(index);
        assert eligibleToCheckForRefresh(idxMeta) : "index " + index + " is missing crucial information needed to refresh phase definition";

        logger.trace("[{}] updating cached phase definition for policy [{}]", index, updatedPolicy.getName());
        LifecycleExecutionState currentExState = LifecycleExecutionState.fromIndexMetadata(idxMeta);

        String currentPhase = currentExState.getPhase();
        PhaseExecutionInfo pei = new PhaseExecutionInfo(updatedPolicy.getName(),
            updatedPolicy.getPolicy().getPhases().get(currentPhase), updatedPolicy.getVersion(), updatedPolicy.getModifiedDate());

        LifecycleExecutionState newExState = LifecycleExecutionState.builder(currentExState)
            .setPhaseDefinition(Strings.toString(pei, false, false))
            .build();

        return IndexLifecycleTransition.newClusterStateWithLifecycleState(idxMeta.getIndex(), state, newExState).build();
    }

    /**
     * For the given new policy, returns a new cluster with all updateable indices' phase JSON refreshed.
     */
    static ClusterState updateIndicesForPolicy(final ClusterState state, final NamedXContentRegistry xContentRegistry, final Client client,
                                               final LifecyclePolicy oldPolicy, final LifecyclePolicyMetadata newPolicy) {
        assert oldPolicy.getName().equals(newPolicy.getName()) : "expected both policies to have the same id but they were: [" +
            oldPolicy.getName() + "] vs. [" + newPolicy.getName() + "]";

        // No need to update anything if the policies are identical in contents
        if (oldPolicy.equals(newPolicy.getPolicy())) {
            logger.debug("policy [{}] is unchanged and no phase definition refresh is needed", oldPolicy.getName());
            return state;
        }

        final List<String> indicesThatCanBeUpdated =
            StreamSupport.stream(Spliterators.spliteratorUnknownSize(state.metaData().indices().valuesIt(), 0), false)
                .filter(meta -> newPolicy.getName().equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(meta.getSettings())))
                .filter(meta -> isIndexPhaseDefinitionUpdatable(xContentRegistry, client, meta, newPolicy.getPolicy()))
                .map(meta -> meta.getIndex().getName())
                .collect(Collectors.toList());

        ClusterState updatedState = state;
        for (String index : indicesThatCanBeUpdated) {
            try {
                updatedState = refreshPhaseDefinition(updatedState, index, newPolicy);
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("[{}] unable to refresh phase definition for updated policy [{}]",
                    index, newPolicy.getName()), e);
            }
        }

        return updatedState;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
