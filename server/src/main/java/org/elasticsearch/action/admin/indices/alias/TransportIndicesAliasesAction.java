/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasAction.AddDataStreamAlias;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexAliasesService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;

/**
 * Add/remove aliases action
 */
public class TransportIndicesAliasesAction extends AcknowledgedTransportMasterNodeAction<IndicesAliasesRequest> {

    public static final String NAME = "indices:admin/aliases";
    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportIndicesAliasesAction.class);

    private final MetadataIndexAliasesService indexAliasesService;
    private final RequestValidators<IndicesAliasesRequest> requestValidators;
    private final SystemIndices systemIndices;

    @Inject
    public TransportIndicesAliasesAction(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final MetadataIndexAliasesService indexAliasesService,
        final ActionFilters actionFilters,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final RequestValidators<IndicesAliasesRequest> requestValidators,
        final SystemIndices systemIndices
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndicesAliasesRequest::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexAliasesService = indexAliasesService;
        this.requestValidators = Objects.requireNonNull(requestValidators);
        this.systemIndices = systemIndices;
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesAliasesRequest request, ClusterState state) {
        Set<String> indices = new HashSet<>();
        for (AliasActions aliasAction : request.aliasActions()) {
            Collections.addAll(indices, aliasAction.indices());
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices.toArray(new String[indices.size()]));
    }

    @Override
    protected void masterOperation(
        Task task,
        final IndicesAliasesRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {

        // Expand the indices names
        List<AliasActions> actions = request.aliasActions();
        List<AliasAction> finalActions = new ArrayList<>();
        // Resolve all the AliasActions into AliasAction instances and gather all the aliases
        Set<String> aliases = new HashSet<>();
        for (AliasActions action : actions) {
            List<String> concreteDataStreams = indexNameExpressionResolver.dataStreamNames(
                state,
                request.indicesOptions(),
                action.indices()
            );
            final Index[] concreteIndices;
            if (concreteDataStreams.size() != 0) {
                Index[] unprocessedConcreteIndices = indexNameExpressionResolver.concreteIndices(
                    state,
                    request.indicesOptions(),
                    true,
                    action.indices()
                );
                List<Index> nonBackingIndices = Arrays.stream(unprocessedConcreteIndices).filter(index -> {
                    var ia = state.metadata().getIndicesLookup().get(index.getName());
                    return ia.getParentDataStream() == null;
                }).toList();
                concreteIndices = nonBackingIndices.toArray(Index[]::new);
                switch (action.actionType()) {
                    case ADD -> {
                        // Fail if parameters are used that data stream aliases don't support:
                        if (action.routing() != null) {
                            throw new IllegalArgumentException("aliases that point to data streams don't support routing");
                        }
                        if (action.indexRouting() != null) {
                            throw new IllegalArgumentException("aliases that point to data streams don't support index_routing");
                        }
                        if (action.searchRouting() != null) {
                            throw new IllegalArgumentException("aliases that point to data streams don't support search_routing");
                        }
                        if (action.isHidden() != null) {
                            throw new IllegalArgumentException("aliases that point to data streams don't support is_hidden");
                        }
                        // Fail if expressions match both data streams and regular indices:
                        if (nonBackingIndices.isEmpty() == false) {
                            throw new IllegalArgumentException(
                                "expressions "
                                    + Arrays.toString(action.indices())
                                    + " that match with both data streams and regular indices are disallowed"
                            );
                        }
                        for (String dataStreamName : concreteDataStreams) {
                            for (String alias : concreteDataStreamAliases(action, state.metadata(), dataStreamName)) {
                                finalActions.add(new AddDataStreamAlias(alias, dataStreamName, action.writeIndex(), action.filter()));
                            }
                        }
                        continue;
                    }
                    case REMOVE -> {
                        for (String dataStreamName : concreteDataStreams) {
                            for (String alias : concreteDataStreamAliases(action, state.metadata(), dataStreamName)) {
                                finalActions.add(new AliasAction.RemoveDataStreamAlias(alias, dataStreamName, action.mustExist()));
                            }
                        }
                        if (nonBackingIndices.isEmpty() == false) {
                            // Regular aliases/indices match as well with the provided expression.
                            // (Only when adding new aliases, matching both data streams and indices is disallowed)
                        } else {
                            continue;
                        }
                    }
                    default -> throw new IllegalArgumentException("Unsupported action [" + action.actionType() + "]");
                }
            } else {
                concreteIndices = indexNameExpressionResolver.concreteIndices(state, request.indicesOptions(), false, action.indices());
            }

            for (Index concreteIndex : concreteIndices) {
                IndexAbstraction indexAbstraction = state.metadata().getIndicesLookup().get(concreteIndex.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + concreteIndex.getName() + "] was not found";
                if (indexAbstraction.getParentDataStream() != null) {
                    throw new IllegalArgumentException(
                        "The provided expressions ["
                            + String.join(",", action.indices())
                            + "] match a backing index belonging to data stream ["
                            + indexAbstraction.getParentDataStream().getName()
                            + "]. Data stream backing indices don't support aliases."
                    );
                }
            }
            final Optional<Exception> maybeException = requestValidators.validateRequest(request, state, concreteIndices);
            if (maybeException.isPresent()) {
                listener.onFailure(maybeException.get());
                return;
            }

            Collections.addAll(aliases, action.getOriginalAliases());
            long now = System.currentTimeMillis();
            for (final Index index : concreteIndices) {
                switch (action.actionType()) {
                    case ADD:
                        for (String alias : concreteAliases(action, state.metadata(), index.getName())) {
                            String resolvedName = IndexNameExpressionResolver.resolveDateMathExpression(alias, now);
                            finalActions.add(
                                new AliasAction.Add(
                                    index.getName(),
                                    resolvedName,
                                    action.filter(),
                                    action.indexRouting(),
                                    action.searchRouting(),
                                    action.writeIndex(),
                                    systemIndices.isSystemName(resolvedName) ? Boolean.TRUE : action.isHidden()
                                )
                            );
                        }
                        break;
                    case REMOVE:
                        for (String alias : concreteAliases(action, state.metadata(), index.getName())) {
                            finalActions.add(new AliasAction.Remove(index.getName(), alias, action.mustExist()));
                        }
                        break;
                    case REMOVE_INDEX:
                        finalActions.add(new AliasAction.RemoveIndex(index.getName()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported action [" + action.actionType() + "]");
                }
            }
        }
        if (finalActions.isEmpty() && false == actions.isEmpty()) {
            throw new AliasesNotFoundException(aliases.toArray(new String[aliases.size()]));
        }
        request.aliasActions().clear();
        IndicesAliasesClusterStateUpdateRequest updateRequest = new IndicesAliasesClusterStateUpdateRequest(unmodifiableList(finalActions))
            .ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout());

        indexAliasesService.indicesAliases(updateRequest, listener.delegateResponse((l, e) -> {
            logger.debug("failed to perform aliases", e);
            l.onFailure(e);
        }));
    }

    private static String[] concreteAliases(AliasActions action, Metadata metadata, String concreteIndex) {
        if (action.expandAliasesWildcards()) {
            // for DELETE we expand the aliases
            String[] concreteIndices = { concreteIndex };
            Map<String, List<AliasMetadata>> aliasMetadata = metadata.findAliases(action.aliases(), concreteIndices);
            List<String> finalAliases = new ArrayList<>();
            for (List<AliasMetadata> aliases : aliasMetadata.values()) {
                for (AliasMetadata aliasMeta : aliases) {
                    finalAliases.add(aliasMeta.alias());
                }
            }
            if (finalAliases.isEmpty() && action.mustExist() != null && action.mustExist()) {
                return action.aliases();
            }
            return finalAliases.toArray(new String[finalAliases.size()]);
        } else {
            // for ADD and REMOVE_INDEX we just return the current aliases
            return action.aliases();
        }
    }

    private static String[] concreteDataStreamAliases(AliasActions action, Metadata metadata, String concreteDataStreamName) {
        if (action.expandAliasesWildcards()) {
            // for DELETE we expand the aliases
            Stream<String> stream = metadata.dataStreamAliases()
                .values()
                .stream()
                .filter(alias -> alias.getDataStreams().contains(concreteDataStreamName))
                .map(DataStreamAlias::getName);

            String[] aliasPatterns = action.aliases();
            if (Strings.isAllOrWildcard(aliasPatterns) == false) {
                stream = stream.filter(alias -> Regex.simpleMatch(aliasPatterns, alias));
            }

            return stream.toArray(String[]::new);
        } else {
            // for ADD and REMOVE_INDEX we just return the current aliases
            return action.aliases();
        }
    }
}
