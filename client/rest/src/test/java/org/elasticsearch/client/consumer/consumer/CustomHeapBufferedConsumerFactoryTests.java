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

package org.elasticsearch.client.consumer.consumer;

import org.apache.http.HttpResponse;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.client.HeapBufferedAsyncResponseConsumer;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RestClientTestCase;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CustomHeapBufferedConsumerFactoryTests extends RestClientTestCase {

    //Note that this test class should be in a different package than org.elasticsearch.client so we can test that
    //the access modifiers are public rather than package private or protected
    public void testCanConfigureHeapBufferLimit() {
        int bufferLimit = randomIntBetween(1, Integer.MAX_VALUE);
        HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(bufferLimit);
        HttpAsyncResponseConsumer<HttpResponse> consumer = consumerFactory.createHttpAsyncResponseConsumer();
        assertThat(consumer, instanceOf(HeapBufferedAsyncResponseConsumer.class));
        HeapBufferedAsyncResponseConsumer bufferedAsyncResponseConsumer = (HeapBufferedAsyncResponseConsumer) consumer;
        assertEquals(bufferLimit, bufferedAsyncResponseConsumer.getBufferLimit());
    }
}
