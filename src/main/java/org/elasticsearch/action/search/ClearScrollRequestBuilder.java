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

package org.elasticsearch.action.search;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.List;

/**
 */
public class ClearScrollRequestBuilder extends ActionRequestBuilder<ClearScrollRequest, ClearScrollResponse, ClearScrollRequestBuilder, Client> {

    private List<String> scrollIds;

    public ClearScrollRequestBuilder(Client client) {
        super(client, new ClearScrollRequest());
    }

    public ClearScrollRequestBuilder setScrollIds(List<String> cursorIds) {
        scrollIds = cursorIds;
        return this;
    }

    public ClearScrollRequestBuilder addScrollId(String cursorId) {
        if (this.scrollIds == null) {
            this.scrollIds = Lists.newArrayList();
        }
        this.scrollIds.add(cursorId);
        return this;
    }

    public ClearScrollRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    public ClearScrollRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request.source(source);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<ClearScrollResponse> listener) {
        if (scrollIds != null && scrollIds.isEmpty() == false) {
            request.setScrollIds(this.scrollIds);
        }
        client.clearScroll(request, listener);
    }
}
