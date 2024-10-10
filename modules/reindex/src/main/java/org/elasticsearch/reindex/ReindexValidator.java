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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Arrays;
import java.util.List;

public class ReindexValidator {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ReindexValidator.class);
    static final String SORT_DEPRECATED_MESSAGE = "The sort option in reindex is deprecated. "
        + "Instead consider using query filtering to find the desired subset of data.";

    private final CharacterRunAutomaton remoteWhitelist;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver resolver;
    private final AutoCreateIndex autoCreateIndex;

    ReindexValidator(
        Settings settings,
        ClusterService clusterService,
        IndexNameExpressionResolver resolver,
        AutoCreateIndex autoCreateIndex
    ) {
        this.remoteWhitelist = buildRemoteWhitelist(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.get(settings));
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.autoCreateIndex = autoCreateIndex;
    }

    public void initialValidation(ReindexRequest request) {
        checkRemoteWhitelist(remoteWhitelist, request.getRemoteInfo());
        ClusterState state = clusterService.state();
        validateAgainstAliases(
            request.getSearchRequest(),
            request.getDestination(),
            request.getRemoteInfo(),
            resolver,
            autoCreateIndex,
            state
        );
        SearchSourceBuilder searchSource = request.getSearchRequest().source();
        if (searchSource != null && searchSource.sorts() != null && searchSource.sorts().isEmpty() == false) {
            deprecationLogger.warn(DeprecationCategory.API, "reindex_sort", SORT_DEPRECATED_MESSAGE);
        }
    }

    static void checkRemoteWhitelist(CharacterRunAutomaton whitelist, RemoteInfo remoteInfo) {
        if (remoteInfo == null) {
            return;
        }
        String check = remoteInfo.getHost() + ':' + remoteInfo.getPort();
        if (whitelist.run(check)) {
            return;
        }
        String whiteListKey = TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey();
        throw new IllegalArgumentException('[' + check + "] not whitelisted in " + whiteListKey);
    }

    /**
     * Build the {@link CharacterRunAutomaton} that represents the reindex-from-remote whitelist and make sure that it doesn't whitelist
     * the world.
     */
    static CharacterRunAutomaton buildRemoteWhitelist(List<String> whitelist) {
        if (whitelist.isEmpty()) {
            return new CharacterRunAutomaton(Automata.makeEmpty());
        }
        Automaton automaton = Regex.simpleMatchToAutomaton(whitelist.toArray(Strings.EMPTY_ARRAY));
        automaton = Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        if (Operations.isTotal(automaton)) {
            throw new IllegalArgumentException(
                "Refusing to start because whitelist "
                    + whitelist
                    + " accepts all addresses. "
                    + "This would allow users to reindex-from-remote any URL they like effectively having Elasticsearch make HTTP GETs "
                    + "for them."
            );
        }
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
        ClusterState clusterState
    ) {
        if (remoteInfo != null) {
            return;
        }
        String target = destination.index();
        if (destination.isRequireAlias() && (false == clusterState.getMetadata().hasAlias(target))) {
            throw new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + target + "] is not an alias",
                target
            );
        }
        if (false == autoCreateIndex.shouldAutoCreate(target, clusterState)) {
            /*
             * If we're going to autocreate the index we don't need to resolve
             * it. This is the same sort of dance that TransportIndexRequest
             * uses to decide to autocreate the index.
             */
            target = indexNameExpressionResolver.concreteWriteIndex(clusterState, destination).getName();
        }
        SearchRequest filteredSource = skipRemoteIndexNames(source);
        if (filteredSource.indices().length == 0) {
            return;
        }
        String[] sourceIndexNames = indexNameExpressionResolver.concreteIndexNames(clusterState, filteredSource);
        for (String sourceIndex : sourceIndexNames) {
            if (sourceIndex.equals(target)) {
                ActionRequestValidationException e = new ActionRequestValidationException();
                e.addValidationError("reindex cannot write into an index its reading from [" + target + ']');
                throw e;
            }
        }
    }

    private static SearchRequest skipRemoteIndexNames(SearchRequest source) {
        // An index expression that references a remote cluster uses ":" to separate the cluster-alias from the index portion of the
        // expression, e.g., cluster0:index-name
        return new SearchRequest(source).indices(
            Arrays.stream(source.indices()).filter(name -> RemoteClusterAware.isRemoteIndexName(name) == false).toArray(String[]::new)
        );
    }
}
