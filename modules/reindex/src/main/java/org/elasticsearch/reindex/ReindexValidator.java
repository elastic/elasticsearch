/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Arrays;
import java.util.List;

public class ReindexValidator {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ReindexValidator.class);
    static final String SORT_DEPRECATED_MESSAGE = "The sort option in reindex is deprecated. "
        + "Instead consider using query filtering to find the desired subset of data.";

    private final CharacterRunAutomaton allowedRemotes;
    private final boolean remoteBlocklistSettingInUse;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexResolver;
    private final ProjectResolver projectResolver;
    private final AutoCreateIndex autoCreateIndex;

    ReindexValidator(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver indexResolver,
        ProjectResolver projectResolver,
        AutoCreateIndex autoCreateIndex
    ) {
        List<String> remoteWhitelist = TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(settings);
        List<String> remoteBlocklist = TransportReindexAction.REMOTE_CLUSTER_BLOCKLIST.get(settings);
        this.allowedRemotes = buildAllowedRemotes(remoteWhitelist, remoteBlocklist);
        this.remoteBlocklistSettingInUse = !TransportReindexAction.REMOTE_CLUSTER_BLOCKLIST.get(settings).isEmpty();
        this.clusterService = clusterService;
        this.indexResolver = indexResolver;
        this.projectResolver = projectResolver;
        this.autoCreateIndex = autoCreateIndex;
    }

    public void initialValidation(ReindexRequest request) {
        checkAllowedRemote(allowedRemotes, remoteBlocklistSettingInUse, request.getRemoteInfo());
        ClusterState state = clusterService.state();
        SearchRequest source = request.getSearchRequest();

        if (source.indicesOptions().resolveCrossProjectIndexExpression() == false
            && request.getRemoteInfo() == null
            && source.getProjectRouting() != null) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError(
                "reindex doesn't support project routing [" + source.getProjectRouting() + "] when cross-project search is disabled"
            );
            throw e;
        }

        final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        validateAgainstAliases(source, request.getDestination(), request.getRemoteInfo(), indexResolver, autoCreateIndex, projectMetadata);
        if (SliceIndexing.SLICE_FEATURE_FLAG.isEnabled()) {
            validateSliceRouting(request, projectMetadata);
        }
        SearchSourceBuilder searchSource = source.source();
        if (searchSource != null && searchSource.sorts() != null && searchSource.sorts().isEmpty() == false) {
            deprecationLogger.warn(DeprecationCategory.API, "reindex_sort", SORT_DEPRECATED_MESSAGE);
        }
    }

    /**
     * Validates the interaction between the source {@code slice} (which slice of a slice-enabled source to read) and the destination
     * {@code slice} (which slice every reindexed document is written to). A destination {@code slice} may be omitted for a slice-enabled
     * destination as long as the source is read in slice mode: in that case each document preserves the slice it was read from.
     */
    private void validateSliceRouting(ReindexRequest request, ProjectMetadata projectMetadata) {
        final IndexRequest destination = request.getDestination();
        final String destinationIndex = destination.index();
        final boolean destinationSliceEnabled = isDestinationSliceEnabled(destination, destinationIndex, projectMetadata);
        final boolean destSliceProvided = destination.isRoutingFromSlice();
        final boolean sourceSliceMode = request.getSearchRequest().isRoutingFromSlice();

        validateNoRequiredRoutingMixedWithSlices(
            request,
            projectMetadata,
            destination,
            destinationIndex,
            destinationSliceEnabled,
            destSliceProvided,
            sourceSliceMode
        );

        if (destSliceProvided && destinationSliceEnabled == false) {
            throw new IllegalArgumentException(
                "["
                    + SliceIndexing.PARAM_NAME
                    + "] is not allowed in [dest] when ["
                    + IndexSettings.SLICE_ENABLED.getKey()
                    + "] is false for destination ["
                    + destinationIndex
                    + "]"
            );
        }
        if (destinationSliceEnabled) {
            if (destSliceProvided == false && destination.routing() != null) {
                throw new IllegalArgumentException(
                    "[routing] is not allowed in [dest] when ["
                        + IndexSettings.SLICE_ENABLED.getKey()
                        + "] is true for destination ["
                        + destinationIndex
                        + "], use ["
                        + SliceIndexing.PARAM_NAME
                        + "] instead"
                );
            }
            // Omitting [slice] in [dest] only works when the source is read in slice mode, so each document can keep its source slice.
            if (destSliceProvided == false && sourceSliceMode == false) {
                throw new IllegalArgumentException(
                    "["
                        + SliceIndexing.PARAM_NAME
                        + "] is required in [dest] when ["
                        + IndexSettings.SLICE_ENABLED.getKey()
                        + "] is true for destination ["
                        + destinationIndex
                        + "] unless the source is read with ["
                        + SliceIndexing.PARAM_NAME
                        + "]"
                );
            }
        }
    }

    /**
     * Slice-enabled indices and indices that require {@code routing} via an explicit {@code _routing: {required: true}} mapping are two
     * distinct routing models that cannot be reconciled during reindex: the former routes documents by their {@code slice}, the latter by
     * an arbitrary user-supplied {@code routing} value. When slices are involved on either side of the reindex, reject any participating
     * non-slice index that requires routing rather than silently producing an unusable destination.
     */
    private void validateNoRequiredRoutingMixedWithSlices(
        ReindexRequest request,
        ProjectMetadata projectMetadata,
        IndexRequest destination,
        String destinationIndex,
        boolean destinationSliceEnabled,
        boolean destSliceProvided,
        boolean sourceSliceMode
    ) {
        boolean anySourceSliceEnabled = false;
        boolean sourceRequiresRouting = false;
        // Remote sources (reindex-from-remote and cross-cluster/cross-project index expressions) cannot be resolved against the local
        // cluster state, and doing so throws "Cross-cluster calls are not supported in this context". Skip remote index names, mirroring
        // validateAgainstAliases, and only inspect the local source indices for slice/required-routing settings.
        if (request.getRemoteInfo() == null) {
            final SearchRequest localSource = skipRemoteIndexNames(request.getSearchRequest());
            if (localSource.indices().length > 0) {
                for (Index index : indexResolver.concreteIndices(projectMetadata, localSource)) {
                    final IndexMetadata indexMetadata = projectMetadata.index(index);
                    if (indexMetadata == null) {
                        continue;
                    }
                    if (IndexSettings.SLICE_ENABLED.get(indexMetadata.getSettings())) {
                        anySourceSliceEnabled = true;
                    } else if (routingRequired(indexMetadata)) {
                        sourceRequiresRouting = true;
                    }
                }
            }
        }

        final boolean slicesInvolved = sourceSliceMode || destSliceProvided || destinationSliceEnabled || anySourceSliceEnabled;
        if (slicesInvolved == false) {
            return;
        }

        if (sourceRequiresRouting) {
            throw new IllegalArgumentException(
                "reindex from an index that requires [routing] is not supported when a [slice] is involved; slice-enabled indices and "
                    + "indices with required [routing] must not be mixed"
            );
        }

        if (destinationSliceEnabled == false) {
            final IndexMetadata destinationMetadata = existingDestinationMetadata(destination, destinationIndex, projectMetadata);
            if (destinationMetadata != null && routingRequired(destinationMetadata)) {
                throw new IllegalArgumentException(
                    "reindex into destination ["
                        + destinationIndex
                        + "] that requires [routing] is not supported when a [slice] is involved; slice-enabled indices and indices with "
                        + "required [routing] must not be mixed"
                );
            }
        }
    }

    private static boolean routingRequired(IndexMetadata indexMetadata) {
        return indexMetadata.mapping() != null && indexMetadata.mapping().routingRequired();
    }

    private IndexMetadata existingDestinationMetadata(IndexRequest destination, String destinationIndex, ProjectMetadata projectMetadata) {
        if (autoCreateIndex.shouldAutoCreate(destinationIndex, projectMetadata)) {
            return null;
        }
        final Index writeIndex = indexResolver.concreteWriteIndex(projectMetadata, destination);
        return projectMetadata.index(writeIndex);
    }

    private boolean isDestinationSliceEnabled(IndexRequest destination, String destinationIndex, ProjectMetadata projectMetadata) {
        if (autoCreateIndex.shouldAutoCreate(destinationIndex, projectMetadata) == false) {
            final Index writeIndex = indexResolver.concreteWriteIndex(projectMetadata, destination);
            final IndexMetadata indexMetadata = projectMetadata.index(writeIndex);
            return indexMetadata != null && IndexSettings.SLICE_ENABLED.get(indexMetadata.getSettings());
        }

        final String templateName = MetadataIndexTemplateService.findV2Template(projectMetadata, destinationIndex, false);
        if (templateName != null) {
            final Settings resolvedSettings = MetadataIndexTemplateService.resolveSettings(projectMetadata, templateName);
            return IndexSettings.SLICE_ENABLED.get(resolvedSettings);
        }
        final List<IndexTemplateMetadata> templates = MetadataIndexTemplateService.findV1Templates(projectMetadata, destinationIndex, null);
        if (templates.isEmpty()) {
            return false;
        }
        final Settings resolvedSettings = MetadataIndexTemplateService.resolveSettings(templates);
        return IndexSettings.SLICE_ENABLED.get(resolvedSettings);
    }

    static void checkAllowedRemote(CharacterRunAutomaton allowedRemotes, boolean remoteBlocklistSettingInUse, RemoteInfo remoteInfo) {
        if (remoteInfo == null) {
            return;
        }
        String check = remoteInfo.getHost() + ':' + remoteInfo.getPort();
        if (allowedRemotes.run(check)) {
            return;
        }
        throw new IllegalArgumentException(
            remoteBlocklistSettingInUse
                ? Strings.format(
                    "[%s] either not whitelisted in %s or blocked in %s",
                    check,
                    TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(),
                    TransportReindexAction.REMOTE_CLUSTER_BLOCKLIST.getKey()
                )
                : Strings.format("[%s] not whitelisted in %s", check, TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey())
        );
    }

    /**
     * Build the {@link CharacterRunAutomaton} that represents the reindex-from-remote whitelist and blocklist and make sure that it doesn't
     * whitelist the world.
     */
    static CharacterRunAutomaton buildAllowedRemotes(List<String> whitelist, List<String> blocklist) {
        if (whitelist.isEmpty()) {
            return new CharacterRunAutomaton(Automata.makeEmpty());
        }
        Automaton automaton = Regex.simpleMatchToAutomaton(whitelist.toArray(String[]::new));
        if (!blocklist.isEmpty()) {
            Automaton toBlock = Regex.simpleMatchToAutomaton(blocklist.toArray(String[]::new));
            automaton = Operations.minus(automaton, toBlock, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        automaton = Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        return new CharacterRunAutomaton(automaton);
    }

    /**
     * Throws an ActionRequestValidationException if the request tries to index
     * back into the same index or into an index that points to two indexes.
     * This cannot be done during request validation because the cluster state
     * isn't available then. Package private for testing.
     */
    static void validateAgainstAliases(
        SearchRequest source,
        IndexRequest destination,
        RemoteInfo remoteInfo,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AutoCreateIndex autoCreateIndex,
        ProjectMetadata project
    ) {
        if (remoteInfo != null) {
            return;
        }
        String target = destination.index();
        if (destination.isRequireAlias() && (false == project.hasAlias(target))) {
            throw new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + target + "] is not an alias",
                target
            );
        }
        if (false == autoCreateIndex.shouldAutoCreate(target, project)) {
            /*
             * If we're going to autocreate the index we don't need to resolve
             * it. This is the same sort of dance that TransportIndexRequest
             * uses to decide to autocreate the index.
             */
            target = indexNameExpressionResolver.concreteWriteIndex(project, destination).getName();
        }
        SearchRequest filteredSource = skipRemoteIndexNames(source);
        if (filteredSource.indices().length == 0) {
            return;
        }
        String[] sourceIndexNames = indexNameExpressionResolver.concreteIndexNames(project, filteredSource);
        for (String sourceIndex : sourceIndexNames) {
            if (sourceIndex.equals(target)) {
                ActionRequestValidationException e = new ActionRequestValidationException();
                e.addValidationError("reindex cannot write into an index its reading from [" + target + ']');
                throw e;
            }
        }
    }

    private static SearchRequest skipRemoteIndexNames(SearchRequest source) {
        IndicesOptions indicesOptions = source.indicesOptions();
        if (indicesOptions.resolveCrossProjectIndexExpression()) {
            indicesOptions = CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout(indicesOptions);
        }
        // An index expression that references a remote cluster uses ":" to separate the cluster-alias from the index portion of the
        // expression, e.g., cluster0:index-name
        return new SearchRequest(source).indicesOptions(indicesOptions)
            .indices(
                Arrays.stream(source.indices()).filter(name -> RemoteClusterAware.isRemoteIndexName(name) == false).toArray(String[]::new)
            );
    }
}
