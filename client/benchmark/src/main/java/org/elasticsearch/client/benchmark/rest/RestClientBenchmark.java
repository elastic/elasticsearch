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

import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.benchmark.ClientBenchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;

public final class RestClientBenchmark extends ClientBenchmark {
    @Override
    protected BulkRequestExecutor requestExecutor(String hostName, String indexName, String typeName) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        //TODO dm: The client needs to be closed at the end of a benchmark
        RestClient client = RestClient.builder(new HttpHost(hostName, 9200))
            .setHttpClient(httpClient)
            .build();
        return new RestBulkRequestExecutor(client, indexName, typeName);
    }

    public static void main(String[] args) throws Exception {
        RestClientBenchmark benchmark = new RestClientBenchmark();
        benchmark.run(args);
    }
}
