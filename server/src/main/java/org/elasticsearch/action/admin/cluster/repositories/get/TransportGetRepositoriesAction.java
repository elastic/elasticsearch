/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transport action for get repositories operation
 */
public class TransportGetRepositoriesAction extends TransportMasterNodeReadAction<GetRepositoriesRequest, GetRepositoriesResponse> {

    @Inject
    public TransportGetRepositoriesAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetRepositoriesAction.NAME, transportService, clusterService, threadPool, actionFilters,
              GetRepositoriesRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetRepositoriesResponse read(StreamInput in) throws IOException {
        return new GetRepositoriesResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetRepositoriesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(Task task, final GetRepositoriesRequest request, ClusterState state,
                                   final ActionListener<GetRepositoriesResponse> listener) {
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
        Metadata metadata = state.metadata();
        RepositoriesMetadata repositories = metadata.custom(RepositoriesMetadata.TYPE);
        if (repoNames.length == 0 || (repoNames.length == 1 && "_all".equals(repoNames[0]))) {
            if (repositories != null) {
                return repositories.repositories();
            } else {
                return Collections.emptyList();
            }
        } else {
            if (repositories != null) {
                Set<String> repositoriesToGet = new LinkedHashSet<>(); // to keep insertion order
                for (String repositoryOrPattern : repoNames) {
                    if (Regex.isSimpleMatchPattern(repositoryOrPattern) == false) {
                        repositoriesToGet.add(repositoryOrPattern);
                    } else {
                        for (RepositoryMetadata repository : repositories.repositories()) {
                            if (Regex.simpleMatch(repositoryOrPattern, repository.name())) {
                                repositoriesToGet.add(repository.name());
                            }
                        }
                    }
                }
                List<RepositoryMetadata> repositoryListBuilder = new ArrayList<>();
                for (String repository : repositoriesToGet) {
                    RepositoryMetadata repositoryMetadata = repositories.repository(repository);
                    if (repositoryMetadata == null) {
                        throw new RepositoryMissingException(repository);
                    }
                    repositoryListBuilder.add(repositoryMetadata);
                }
                return repositoryListBuilder;
            } else {
                throw new RepositoryMissingException(repoNames[0]);
            }
        }
    }
}
