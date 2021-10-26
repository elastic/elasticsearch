/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transport action for get repositories operation
 */
public class TransportGetRepositoriesAction extends TransportMasterNodeReadAction<GetRepositoriesRequest, GetRepositoriesResponse> {

    public static final String ALL_PATTERN = "_all";

    @Inject
    public TransportGetRepositoriesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetRepositoriesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetRepositoriesRequest::new,
            indexNameExpressionResolver,
            GetRepositoriesResponse::new,
            ThreadPool.Names.SAME
        );
    }

    public static boolean isMatchAll(String[] patterns) {
        return (patterns.length == 0)
            || (patterns.length == 1 && (ALL_PATTERN.equalsIgnoreCase(patterns[0]) || Regex.isMatchAllPattern(patterns[0])));
    }

    @Override
    protected ClusterBlockException checkBlock(GetRepositoriesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final GetRepositoriesRequest request,
        ClusterState state,
        final ActionListener<GetRepositoriesResponse> listener
    ) {
        listener.onResponse(new GetRepositoriesResponse(new RepositoriesMetadata(getRepositories(state, request.repositories()))));
    }

    /**
     * Get repository metadata for given repository names from given cluster state.
     *
     * @param state     Cluster state
     * @param repoNames Repository names or patterns to get metadata for
     * @return list of repository metadata
     */
    public static List<RepositoryMetadata> getRepositories(ClusterState state, String[] repoNames) {
        RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        if (isMatchAll(repoNames)) {
            return repositories.repositories();
        }
        final List<String> includePatterns = new ArrayList<>();
        final List<String> excludePatterns = new ArrayList<>();
        boolean seenWildcard = false;
        for (String repositoryOrPattern : repoNames) {
            if (seenWildcard && repositoryOrPattern.length() > 1 && repositoryOrPattern.startsWith("-")) {
                excludePatterns.add(repositoryOrPattern.substring(1));
            } else {
                if (Regex.isSimpleMatchPattern(repositoryOrPattern)) {
                    seenWildcard = true;
                } else {
                    if (repositories.repository(repositoryOrPattern) == null) {
                        throw new RepositoryMissingException(repositoryOrPattern);
                    }
                }
                includePatterns.add(repositoryOrPattern);
            }
        }
        final String[] excludes = excludePatterns.toArray(Strings.EMPTY_ARRAY);
        final Set<RepositoryMetadata> repositoryListBuilder = new LinkedHashSet<>(); // to keep insertion order
        for (String repositoryOrPattern : includePatterns) {
            for (RepositoryMetadata repository : repositories.repositories()) {
                if (repositoryListBuilder.contains(repository) == false
                    && Regex.simpleMatch(repositoryOrPattern, repository.name())
                    && Regex.simpleMatch(excludes, repository.name()) == false) {
                    repositoryListBuilder.add(repository);
                }
            }
        }
        return List.copyOf(repositoryListBuilder);
    }
}
