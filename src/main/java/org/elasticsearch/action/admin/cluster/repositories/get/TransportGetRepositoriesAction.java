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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for get repositories operation
 */
public class TransportGetRepositoriesAction extends TransportMasterNodeReadOperationAction<GetRepositoriesRequest, GetRepositoriesResponse> {

    @Inject
    public TransportGetRepositoriesAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, GetRepositoriesAction.NAME, transportService, clusterService, threadPool, actionFilters, GetRepositoriesRequest.class);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected GetRepositoriesResponse newResponse() {
        return new GetRepositoriesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(GetRepositoriesRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_READ, "");
    }

    @Override
    protected void masterOperation(final GetRepositoriesRequest request, ClusterState state, final ActionListener<GetRepositoriesResponse> listener) throws ElasticsearchException {
        MetaData metaData = state.metaData();
        RepositoriesMetaData repositories = metaData.custom(RepositoriesMetaData.TYPE);
        if (request.repositories().length == 0 || (request.repositories().length == 1 && "_all".equals(request.repositories()[0]))) {
            if (repositories != null) {
                listener.onResponse(new GetRepositoriesResponse(repositories.repositories()));
            } else {
                listener.onResponse(new GetRepositoriesResponse(ImmutableList.<RepositoryMetaData>of()));
            }
        } else {
            if (repositories != null) {
                ImmutableList.Builder<RepositoryMetaData> repositoryListBuilder = ImmutableList.builder();
                for (String repository : request.repositories()) {
                    RepositoryMetaData repositoryMetaData = repositories.repository(repository);
                    if (repositoryMetaData == null) {
                        listener.onFailure(new RepositoryMissingException(repository));
                        return;
                    }
                    repositoryListBuilder.add(repositoryMetaData);
                }
                listener.onResponse(new GetRepositoriesResponse(repositoryListBuilder.build()));
            } else {
                listener.onFailure(new RepositoryMissingException(request.repositories()[0]));
            }
        }
    }
}
