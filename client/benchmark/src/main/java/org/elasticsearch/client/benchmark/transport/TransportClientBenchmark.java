/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.benchmark.AbstractBenchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.search.SearchRequestExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugin.noop.NoopPlugin;
import org.elasticsearch.plugin.noop.action.bulk.NoopBulkAction;
import org.elasticsearch.plugin.noop.action.search.NoopSearchAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xcontent.XContentType;

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
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY, NoopPlugin.class);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName(benchmarkTargetHost), 9300));
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

        TransportBulkRequestExecutor(TransportClient client, String indexName, String typeName) {
            this.client = client;
            this.indexName = indexName;
            this.typeName = typeName;
        }

        @Override
        public boolean bulkIndex(List<String> bulkData) {
            BulkRequest bulkRequest = new BulkRequest();
            for (String bulkItem : bulkData) {
                bulkRequest.add(new IndexRequest(indexName, typeName).source(bulkItem.getBytes(StandardCharsets.UTF_8), XContentType.JSON));
            }
            BulkResponse bulkResponse;
            try {
                bulkResponse = client.execute(NoopBulkAction.INSTANCE, bulkRequest).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                throw new ElasticsearchException(e);
            }
            return bulkResponse.hasFailures() == false;
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
                final SearchRequest searchRequest = new SearchRequest(indexName);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchRequest.source(searchSourceBuilder);
                searchSourceBuilder.query(QueryBuilders.wrapperQuery(source));
                response = client.execute(NoopSearchAction.INSTANCE, searchRequest).get();
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
