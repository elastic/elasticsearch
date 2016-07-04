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

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;

public class ShrinkRequestBuilder extends AcknowledgedRequestBuilder<ShrinkRequest, ShrinkResponse,
    ShrinkRequestBuilder> {
    public ShrinkRequestBuilder(ElasticsearchClient client, ShrinkAction action) {
        super(client, action, new ShrinkRequest());
    }


    public ShrinkRequestBuilder setTargetIndex(CreateIndexRequest request) {
        this.request.setShrinkIndex(request);
        return this;
    }

    public ShrinkRequestBuilder setSourceIndex(String index) {
        this.request.setSourceIndex(index);
        return this;
    }

    public ShrinkRequestBuilder setSettings(Settings settings) {
        this.request.getShrinkIndexRequest().settings(settings);
        return this;
    }
}
