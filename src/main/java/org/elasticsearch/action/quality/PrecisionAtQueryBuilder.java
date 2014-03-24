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

package org.elasticsearch.action.quality;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.Set;

public class PrecisionAtQueryBuilder extends ActionRequestBuilder<PrecisionAtRequest, PrecisionAtResponse, PrecisionAtQueryBuilder> {
    
    protected PrecisionAtQueryBuilder(Client client) {
        super((InternalClient) client, new PrecisionAtRequest());
    }

    @Override
    protected void doExecute(ActionListener<PrecisionAtResponse> listener) {
    }

    public PrecisionAtQueryBuilder setQuery(BytesReference query) {
        request.queryBuilder(query);
        return this;
    }

    public PrecisionAtQueryBuilder addRelevantDocs(Set<String> relevant) {
        request.relevantDocs(relevant);
        return this;
    }
    
    public PrecisionAtRequest request() {
        return request;
    }
}
