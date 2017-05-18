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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Defines action to collect stats from nodes about shards with phantom engines.
 */
public class PhantomIndicesStatsAction extends Action<PhantomIndicesStatsRequest, PhantomIndicesStatsResponse, PhantomIndicesStatsRequestBuilder> {

    public static final PhantomIndicesStatsAction INSTANCE = new PhantomIndicesStatsAction();
    public static final String NAME = "indices:monitor/stats/phantom";

    private PhantomIndicesStatsAction() {
        super(NAME);
    }

    @Override
    public PhantomIndicesStatsResponse newResponse() {
        return new PhantomIndicesStatsResponse();
    }

    @Override
    public PhantomIndicesStatsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new PhantomIndicesStatsRequestBuilder(client, this);
    }
}
