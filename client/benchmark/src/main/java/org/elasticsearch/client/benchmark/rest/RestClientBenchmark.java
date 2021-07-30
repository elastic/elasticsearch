/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.rest;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.benchmark.AbstractBenchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.search.SearchRequestExecutor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public final class RestClientBenchmark extends AbstractBenchmark<RestClient> {
    public static void main(String[] args) throws Exception {
        RestClientBenchmark b = new RestClientBenchmark();
        b.run(args);
    }

    @Override
    protected RestClient client(String benchmarkTargetHost) {
        return RestClient
            .builder(new HttpHost(benchmarkTargetHost, 9200))
            .setHttpClientConfigCallback(b -> b.setDefaultHeaders(
                Collections.singleton(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "gzip"))))
            .setRequestConfigCallback(b -> b.setContentCompressionEnabled(true))
            .build();
    }

    @Override
    protected BulkRequestExecutor bulkRequestExecutor(RestClient client, String indexName, String typeName) {
        return new RestBulkRequestExecutor(client, indexName, typeName);
    }

    @Override
    protected SearchRequestExecutor searchRequestExecutor(RestClient client, String indexName) {
        return new RestSearchRequestExecutor(client, indexName);
    }

    private static final class RestBulkRequestExecutor implements BulkRequestExecutor {
        private final RestClient client;
        private final String actionMetadata;

        RestBulkRequestExecutor(RestClient client, String index, String type) {
            this.client = client;
            this.actionMetadata = String.format(Locale.ROOT, "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n", index, type);
        }

        @Override
        public boolean bulkIndex(List<String> bulkData) {
            StringBuilder bulkRequestBody = new StringBuilder();
            for (String bulkItem : bulkData) {
                bulkRequestBody.append(actionMetadata);
                bulkRequestBody.append(bulkItem);
                bulkRequestBody.append("\n");
            }
            Request request = new Request("POST", "/geonames/type/_noop_bulk");
            request.setJsonEntity(bulkRequestBody.toString());
            try {
                Response response = client.performRequest(request);
                return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
            } catch (Exception e) {
                throw new ElasticsearchException(e);
            }
        }
    }

    private static final class RestSearchRequestExecutor implements SearchRequestExecutor {
        private final RestClient client;
        private final String endpoint;

        private RestSearchRequestExecutor(RestClient client, String indexName) {
            this.client = client;
            this.endpoint = "/" + indexName + "/_noop_search";
        }

        @Override
        public boolean search(String source) {
            Request request = new Request("GET", endpoint);
            request.setJsonEntity(source);
            try {
                Response response = client.performRequest(request);
                return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
            } catch (IOException e) {
                throw new ElasticsearchException(e);
            }
        }
    }
}
