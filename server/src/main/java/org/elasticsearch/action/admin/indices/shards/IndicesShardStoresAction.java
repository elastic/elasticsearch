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

import org.elasticsearch.action.ActionType;

/**
 * ActionType for {@link TransportIndicesShardStoresAction}
 *
 * Exposes shard store information for requested indices.
 * Shard store information reports which nodes hold shard copies, how recent they are
 * and any exceptions on opening the shard index or from previous engine failures
 */
public class IndicesShardStoresAction extends ActionType<IndicesShardStoresResponse> {

    public static final IndicesShardStoresAction INSTANCE = new IndicesShardStoresAction();
    public static final String NAME = "indices:monitor/shard_stores";

    private IndicesShardStoresAction() {
        super(NAME, IndicesShardStoresResponse::new);
    }
}
