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
package org.elasticsearch.plugin.noop.action.bulk;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.ElasticsearchClient;

public class NoopBulkAction extends Action<BulkRequest, BulkResponse, NoopBulkRequestBuilder> {
    public static final String NAME = "mock:data/write/bulk";

    public static final NoopBulkAction INSTANCE = new NoopBulkAction();

    private NoopBulkAction() {
        super(NAME);
    }

    @Override
    public NoopBulkRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new NoopBulkRequestBuilder(client, this);
    }

    @Override
    public BulkResponse newResponse() {
        return new BulkResponse(null, 0);
    }
}
