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

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class MultiTermVectorsRequestBuilder extends ActionRequestBuilder<MultiTermVectorsRequest, MultiTermVectorsResponse> {

    public MultiTermVectorsRequestBuilder(ElasticsearchClient client, MultiTermVectorsAction action) {
        super(client, action, new MultiTermVectorsRequest());
    }

    public MultiTermVectorsRequestBuilder add(String index, Iterable<String> ids) {
        for (String id : ids) {
            request.add(index, id);
        }
        return this;
    }

    public MultiTermVectorsRequestBuilder add(String index, String... ids) {
        for (String id : ids) {
            request.add(index, id);
        }
        return this;
    }

    public MultiTermVectorsRequestBuilder add(TermVectorsRequest termVectorsRequest) {
        request.add(termVectorsRequest);
        return this;
    }
}
