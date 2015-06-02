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

package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for {@link TransportIndicesUnassignedShardsAction}
 *
 * Exposes information about where unassigned shards are located, how recent they are
 * and reports if the shards can be opened
 */
public class IndicesUnassignedShardsAction extends Action<IndicesUnassignedShardsRequest, IndicesUnassigedShardsResponse, IndicesUnassigedShardsRequestBuilder> {

    public static final IndicesUnassignedShardsAction INSTANCE = new IndicesUnassignedShardsAction();
    public static final String NAME = "indices:monitor/unassigned_shards";

    private IndicesUnassignedShardsAction() {
        super(NAME);
    }

    @Override
    public IndicesUnassigedShardsResponse newResponse() {
        return new IndicesUnassigedShardsResponse();
    }

    @Override
    public IndicesUnassigedShardsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new IndicesUnassigedShardsRequestBuilder(client, this);
    }
}
