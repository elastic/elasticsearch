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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.benchmark.AbstractBenchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.search.SearchRequestExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class TransportClientBenchmark extends AbstractBenchmark<TransportClient> {
    public static void main(String[] args) throws Exception {
        TransportClientBenchmark benchmark = new TransportClientBenchmark();
        benchmark.run(args);
    }

    @Override
    protected TransportClient client(String benchmarkTargetHost) throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(benchmarkTargetHost), 9300));
        return client;
    }

    @Override
    protected BulkRequestExecutor bulkRequestExecutor(TransportClient client, String indexName, String typeName) {
        return new TransportBulkRequestExecutor(client, indexName, typeName);
    }

    @Override
    protected SearchRequestExecutor searchRequestExecutor(TransportClient client, String indexName) {
        return new TransportSearchRequestExecutor(client, indexName);
    }

    private static final class TransportBulkRequestExecutor implements BulkRequestExecutor {
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

    private static final class TransportSearchRequestExecutor implements SearchRequestExecutor {
        private final TransportClient client;
        private final String indexName;

        private TransportSearchRequestExecutor(TransportClient client, String indexName) {
            this.client = client;
            this.indexName = indexName;
        }

        @Override
        public boolean search(String source) {
            final SearchResponse response;
            try {
                response = client.prepareSearch(indexName).setQuery(QueryBuilders.wrapperQuery(source)).execute().get();
                return response.status() == RestStatus.OK;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                throw new ElasticsearchException(e);
            }
        }
    }
}
