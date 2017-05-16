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

package org.elasticsearch.legacy.action.admin.cluster.repositories.get;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.metadata.MetaData;
import org.elasticsearch.legacy.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.legacy.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.repositories.RepositoryMissingException;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 * Transport action for get repositories operation
 */
public class TransportGetRepositoriesAction extends TransportMasterNodeReadOperationAction<GetRepositoriesRequest, GetRepositoriesResponse> {

    @Inject
    public TransportGetRepositoriesAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool) {
        super(settings, GetRepositoriesAction.NAME, transportService, clusterService, threadPool);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected GetRepositoriesRequest newRequest() {
        return new GetRepositoriesRequest();
    }

    @Override
    protected GetRepositoriesResponse newResponse() {
        return new GetRepositoriesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(GetRepositoriesRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, "");
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
