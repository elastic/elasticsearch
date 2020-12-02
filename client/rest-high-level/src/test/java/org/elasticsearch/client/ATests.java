/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class ATests extends ESTestCase {

    Logger logger = LogManager.getLogger(getClass());

    public static final int DOC_COUNT = 4;

    @Test
    public void testGeoIp3() throws IOException {
        final CredentialsProvider credentialsProvider =
            new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials("hesperus", "hesperus"));

        try (RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder("792" +
                ":dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRjZDRmZDc4MjRjY2E0MjA2OWYxNGY2YzFlZDBhMTZiYiQ4MDNmNWM5NTFkYTQ0ZDU5YjMxZGQ3MDYyODhiYmI0OQ" +
                "==").setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)))) {

            byte[] bytes = Files.readAllBytes(new File("/Users/hesperus/Downloads/GeoLite2-City_20201117.tar.gz").toPath());
            IndexResponse geoip_db = client.index(new IndexRequest("geoip_db").id("adf").source("db", bytes), RequestOptions.DEFAULT);

            assertEquals(201, geoip_db.status().getStatus());
        }
    }

    @Test
    public void testGeoIp() throws InterruptedException, IOException {
        final CredentialsProvider credentialsProvider =
            new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials("hesperus", "hesperus"));

        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder("792" +
                ":dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRjZDRmZDc4MjRjY2E0MjA2OWYxNGY2YzFlZDBhMTZiYiQ4MDNmNWM5NTFkYTQ0ZDU5YjMxZGQ3MDYyODhiYmI0OQ" +
                "==").setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)));
        BulkProcessor processor = BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT,
            bulkListener),
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.info(request);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error(request, failure);
                }
            }).setConcurrentRequests(30).setGlobalPipeline("geoip").build();
        for (int i = 0; i < DOC_COUNT; i++) {
            for (int j = 0; j < 255; j++) {
                for (int k = 0; k < 255; k++) {
                    processor.add(new IndexRequest("test2").source(XContentType.JSON, "clientip", "86." + k + "." + j + "." + j));
                }
            }
        }
        processor.flush();
        processor.close();
        processor.awaitClose(10, TimeUnit.SECONDS);
        client.close();
    }

    @Test
    public void testGeoIp2() throws InterruptedException, IOException {
        final CredentialsProvider credentialsProvider =
            new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials("hesperus", "hesperus"));

        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder("792" +
                ":dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRjZDRmZDc4MjRjY2E0MjA2OWYxNGY2YzFlZDBhMTZiYiQ4MDNmNWM5NTFkYTQ0ZDU5YjMxZGQ3MDYyODhiYmI0OQ" +
                "==").setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)));
        BulkProcessor processor = BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT,
            bulkListener),
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.info(request);
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error(request, failure);
                }
            }).setConcurrentRequests(30).setGlobalPipeline("geoip2").build();
        for (int i = 0; i < DOC_COUNT; i++) {
            for (int j = 0; j < 255; j++) {
                for (int k = 0; k < 255; k++) {
                    processor.add(new IndexRequest("test2").source(XContentType.JSON, "clientip", "86." + k + "." + j + "."+j));
                }
            }
        }
        processor.flush();
        processor.close();
        processor.awaitClose(10, TimeUnit.SECONDS);
        client.close();
    }
}
