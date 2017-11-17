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
package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

public class ResizeRequestBuilder extends AcknowledgedRequestBuilder<ResizeRequest, ResizeResponse,
    ResizeRequestBuilder> {
    public ResizeRequestBuilder(ElasticsearchClient client, Action<ResizeRequest, ResizeResponse, ResizeRequestBuilder> action) {
        super(client, action, new ResizeRequest());
    }


    public ResizeRequestBuilder setTargetIndex(CreateIndexRequest request) {
        this.request.setTargetIndex(request);
        return this;
    }

    public ResizeRequestBuilder setSourceIndex(String index) {
        this.request.setSourceIndex(index);
        return this;
    }

    public ResizeRequestBuilder setSettings(Settings settings) {
        this.request.getTargetIndexRequest().settings(settings);
        return this;
    }

    /**
     * Sets the number of shard copies that should be active for creation of the
     * new shrunken index to return. Defaults to {@link ActiveShardCount#DEFAULT}, which will
     * wait for one shard copy (the primary) to become active. Set this value to
     * {@link ActiveShardCount#ALL} to wait for all shards (primary and all replicas) to be active
     * before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link ResizeResponse#isShardsAcked()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public ResizeRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.request.setWaitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public ResizeRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public ResizeRequestBuilder setResizeType(ResizeType type) {
        this.request.setResizeType(type);
        return this;
    }
}
