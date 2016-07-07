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
package org.elasticsearch.client.benchmark.rest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.benchmark.Benchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkBenchmarkTask;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.search.SearchBenchmarkTask;
import org.elasticsearch.client.benchmark.ops.search.SearchRequestExecutor;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public final class RestClientBenchmark {
    private static final int SEARCH_BENCHMARK_ITERATIONS = 10_000;

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println(
                "usage: benchmarkTargetHostIp indexFilePath indexName typeName numberOfDocuments bulkSize [search request body]");
            System.exit(1);
        }
        String benchmarkTargetHost = args[0];
        String indexFilePath = args[1];
        String indexName = args[2];
        String typeName = args[3];
        int totalDocs = Integer.valueOf(args[4]);
        int bulkSize = Integer.valueOf(args[5]);

        int totalIterationCount = (int) Math.floor(totalDocs / bulkSize);
        // consider 40% of all iterations as warmup iterations
        int warmupIterations = (int) (0.4d * totalIterationCount);
        int iterations = totalIterationCount - warmupIterations;
        String searchBody = (args.length == 7) ? args[6] : null;

        CloseableHttpClient httpClient = HttpClients
            .custom()
            .setRetryHandler(new StandardHttpRequestRetryHandler(3, true))
            .build();

        RestClient client = RestClient.builder(new HttpHost(benchmarkTargetHost, 9200))
            .setHttpClient(httpClient)
            .build();

        Benchmark benchmark = new Benchmark(warmupIterations, iterations,
            bulkSize, new BulkBenchmarkTask(
            new RestBulkRequestExecutor(client, indexName, typeName), indexFilePath, warmupIterations + iterations, bulkSize));

        try {
            benchmark.run();
            if (searchBody != null) {
                Benchmark searchBenchmark = new Benchmark(SEARCH_BENCHMARK_ITERATIONS, SEARCH_BENCHMARK_ITERATIONS,
                    new SearchBenchmarkTask(
                        new RestSearchRequestExecutor(client, indexName), searchBody, 2 * SEARCH_BENCHMARK_ITERATIONS));
                searchBenchmark.run();
            }
        } finally {
            httpClient.close();
        }
    }

    private static final class RestBulkRequestExecutor implements BulkRequestExecutor {
        private final RestClient client;
        private final String actionMetaData;

        public RestBulkRequestExecutor(RestClient client, String index, String type) {
            this.client = client;
            this.actionMetaData = String.format(Locale.ROOT, "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n", index, type);
        }

        @Override
        public boolean bulkIndex(List<String> bulkData) {
            StringBuilder bulkRequestBody = new StringBuilder();
            for (String bulkItem : bulkData) {
                bulkRequestBody.append(actionMetaData);
                bulkRequestBody.append(bulkItem);
                bulkRequestBody.append("\n");
            }
            StringEntity entity = new StringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
            Response response = null;
            try {
                response = client.performRequest("POST", "/_bulk", Collections.emptyMap(), entity);
                return response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED;
            } catch (Exception e) {
                throw new ElasticsearchException(e);
            } finally {
                if (response != null) {
                    try {
                        response.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    private static final class RestSearchRequestExecutor implements SearchRequestExecutor {
        private final RestClient client;
        private final String endpoint;

        private RestSearchRequestExecutor(RestClient client, String indexName) {
            this.client = client;
            this.endpoint = "/" + indexName + "/_search";
        }

        @Override
        public boolean search(String source) {
            HttpEntity searchBody = new StringEntity(source, StandardCharsets.UTF_8);
            Response response = null;
            try {
                response = client.performRequest("GET", endpoint, Collections.emptyMap(), searchBody);
                return response.getStatusLine().getStatusCode() == RestStatus.OK.getStatus();
            } catch (IOException e) {
                throw new ElasticsearchException(e);
            } finally {
                try {
                    if (response != null) {
                        response.close();
                    }
                } catch (IOException e) {
                    // close quietly
                }
            }
        }
    }
}
