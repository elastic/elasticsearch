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
            validateDestinationSliceRouting(request, projectMetadata);
        }
        SearchSourceBuilder searchSource = source.source();
        if (searchSource != null && searchSource.sorts() != null && searchSource.sorts().isEmpty() == false) {
            deprecationLogger.warn(DeprecationCategory.API, "reindex_sort", SORT_DEPRECATED_MESSAGE);
        }
    }

    private void validateDestinationSliceRouting(ReindexRequest request, ProjectMetadata projectMetadata) {
        final IndexRequest destination = request.getDestination();
        final String destinationIndex = destination.index();
        final boolean destinationSliceEnabled = isDestinationSliceEnabled(destination, destinationIndex, projectMetadata);

        if (destinationSliceEnabled == false && destination.isRoutingFromSlice()) {
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
            if (destination.isRoutingFromSlice() == false && destination.routing() != null) {
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
            if (destination.routing() == null) {
                throw new IllegalArgumentException(
                    "[" + SliceIndexing.PARAM_NAME + "] is required in [dest] when [" + IndexSettings.SLICE_ENABLED.getKey() + "] is true"
                );
            }
        }
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
