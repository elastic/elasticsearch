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

package org.elasticsearch.action.count;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action that shortcuts to the search api with size set to 0. It doesn't have a corresponding
 * transport action, it just runs the search api internally.
 */
public class CountAction extends Action<CountRequest, CountResponse, CountRequestBuilder> {

    public static final CountAction INSTANCE = new CountAction();
    public static final String NAME = "indices:data/read/count";

    private CountAction() {
        super(NAME);
    }

    @Override
    public CountResponse newResponse() {
        throw new UnsupportedOperationException("CountAction doesn't have its own transport action, gets executed as a SearchAction internally");
    }

    @Override
    public CountRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new CountRequestBuilder(client, this);
    }
}
