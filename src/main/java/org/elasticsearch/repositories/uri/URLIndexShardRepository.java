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

package org.elasticsearch.repositories.uri;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoryName;

/**
 */
public class URLIndexShardRepository extends BlobStoreIndexShardRepository {

    @Inject
    URLIndexShardRepository(Settings settings, RepositoryName repositoryName, IndicesService indicesService, ClusterService clusterService) {
        super(settings, repositoryName, indicesService, clusterService);
    }


    @Override
    public void verify(String seed) {
        //TODO: Add verification that URL is accessible
    }
}
