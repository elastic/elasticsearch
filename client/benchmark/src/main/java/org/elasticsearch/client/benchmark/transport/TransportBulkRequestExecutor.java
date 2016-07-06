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
package org.elasticsearch.client.benchmark.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.transport.TransportClient;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TransportBulkRequestExecutor implements BulkRequestExecutor {
    private final TransportClient client;
    private final String indexName;
    private final String typeName;

    public TransportBulkRequestExecutor(TransportClient client, String indexName, String typeName) {
        this.client = client;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    @Override
    public boolean bulkIndex(List<String> bulkData) {
        BulkRequestBuilder builder = client.prepareBulk();
        for (String bulkItem : bulkData) {
            builder.add(new IndexRequest(indexName, typeName).source(bulkItem.getBytes(StandardCharsets.UTF_8)));
        }
        BulkResponse bulkResponse;
        try {
            bulkResponse = builder.execute().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            throw new ElasticsearchException(e);
        }
        return !bulkResponse.hasFailures();
    }
}
