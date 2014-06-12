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

package org.elasticsearch.action.bulk;

import com.google.common.collect.Maps;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BulkProcessorTests extends ElasticsearchIntegrationTest {

    @Test
    public void testThatBulkProcessorCountIsCorrect() throws InterruptedException {
        final AtomicReference<BulkResponse> responseRef = new AtomicReference<>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failureRef.set(failure);
                latch.countDown();
            }
        };


        try (BulkProcessor processor = BulkProcessor.builder(client(), listener).setBulkActions(5)
                .setConcurrentRequests(1).setName("foo").build()) {
            Map<String, Object> data = Maps.newHashMap();
            data.put("foo", "bar");

            processor.add(new IndexRequest("test", "test", "1").source(data));
            processor.add(new IndexRequest("test", "test", "2").source(data));
            processor.add(new IndexRequest("test", "test", "3").source(data));
            processor.add(new IndexRequest("test", "test", "4").source(data));
            processor.add(new IndexRequest("test", "test", "5").source(data));

            latch.await();
            BulkResponse response = responseRef.get();
            Throwable error = failureRef.get();
            assertThat(error, nullValue());
            assertThat("Could not get a bulk response even after an explicit flush.", response, notNullValue());
            assertThat(response.getItems().length, is(5));
        }
    }

    @Test
    public void testBulkProcessorFlush() throws InterruptedException {
        final AtomicReference<BulkResponse> responseRef = new AtomicReference<>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failureRef.set(failure);
                latch.countDown();
            }
        };

        try (BulkProcessor processor = BulkProcessor.builder(client(), listener).setBulkActions(6)
                .setConcurrentRequests(1).setName("foo").build()) {
            Map<String, Object> data = Maps.newHashMap();
            data.put("foo", "bar");

            processor.add(new IndexRequest("test", "test", "1").source(data));
            processor.add(new IndexRequest("test", "test", "2").source(data));
            processor.add(new IndexRequest("test", "test", "3").source(data));
            processor.add(new IndexRequest("test", "test", "4").source(data));
            processor.add(new IndexRequest("test", "test", "5").source(data));

            processor.flush();
            latch.await();
            BulkResponse response = responseRef.get();
            Throwable error = failureRef.get();
            assertThat(error, nullValue());
            assertThat("Could not get a bulk response even after an explicit flush.", response, notNullValue());
            assertThat(response.getItems().length, is(5));
        }
    }

}
