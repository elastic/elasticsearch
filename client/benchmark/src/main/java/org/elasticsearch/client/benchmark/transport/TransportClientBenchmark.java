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
import org.elasticsearch.client.benchmark.Benchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkBenchmarkTask;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.search.SearchBenchmarkTask;
import org.elasticsearch.client.benchmark.ops.search.SearchRequestExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class TransportClientBenchmark {
    private static final int SEARCH_BENCHMARK_ITERATIONS = 10_000;

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println(
                "usage: benchmarkTargetHostIp indexFilePath indexName typeName numberOfDocuments bulkSize  [search request body]");
            System.exit(1);
        }
        String benchmarkTargetHost = args[0];
        String indexFilePath = args[1];
        String indexName = args[2];
        String typeName = args[3];
        int totalDocs = Integer.valueOf(args[4]);
        int bulkSize = Integer.valueOf(args[5]);
        String searchBody = (args.length == 7) ? args[6] : null;

        int totalIterationCount = (int) Math.floor(totalDocs / bulkSize);
        // consider 10% of all iterations as warmup iterations
        int warmupIterations = (int) (0.1d * totalIterationCount);
        int iterations = totalIterationCount - warmupIterations;

        Settings clientSettings = Settings.builder().put(Node.NODE_MODE_SETTING.getKey(), "network").build();
        TransportClient client = TransportClient.builder().settings(clientSettings).build();
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(benchmarkTargetHost), 9300));

        Benchmark indexBenchmark = new Benchmark(warmupIterations, iterations,bulkSize,
            new BulkBenchmarkTask(
                new TransportBulkRequestExecutor(client, indexName, typeName), indexFilePath, warmupIterations + iterations, bulkSize));
        try {
            indexBenchmark.run();

            if (searchBody != null) {
                Benchmark searchBenchmark = new Benchmark(SEARCH_BENCHMARK_ITERATIONS, SEARCH_BENCHMARK_ITERATIONS,
                    new SearchBenchmarkTask(
                        new TransportSearchRequestExecutor(client, indexName), searchBody, 2 * SEARCH_BENCHMARK_ITERATIONS));
                searchBenchmark.run();
            }
        } finally {
            client.close();
        }
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
