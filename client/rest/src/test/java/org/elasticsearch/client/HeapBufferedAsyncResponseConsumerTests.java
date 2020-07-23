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

package org.elasticsearch.client;

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HeapBufferedAsyncResponseConsumerTests extends RestClientTestCase {

    //maximum buffer that this test ends up allocating is 50MB
    private static final int MAX_TEST_BUFFER_SIZE = 50 * 1024 * 1024;
    private static final int TEST_BUFFER_LIMIT = 10 * 1024 * 1024;

    public void testResponseProcessing() throws Exception {
        ContentDecoder contentDecoder = mock(ContentDecoder.class);
        IOControl ioControl = mock(IOControl.class);
        HttpContext httpContext = mock(HttpContext.class);

        HeapBufferedAsyncResponseConsumer consumer = spy(new HeapBufferedAsyncResponseConsumer(TEST_BUFFER_LIMIT));

        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "OK");
        HttpResponse httpResponse = new BasicHttpResponse(statusLine);
        httpResponse.setEntity(new StringEntity("test", ContentType.TEXT_PLAIN));

        //everything goes well
        consumer.responseReceived(httpResponse);
        consumer.consumeContent(contentDecoder, ioControl);
        consumer.responseCompleted(httpContext);

        verify(consumer).releaseResources();
        verify(consumer).buildResult(httpContext);
        assertTrue(consumer.isDone());
        assertSame(httpResponse, consumer.getResult());

        consumer.responseCompleted(httpContext);
        verify(consumer, times(1)).releaseResources();
        verify(consumer, times(1)).buildResult(httpContext);
    }

    public void testDefaultBufferLimit() throws Exception {
        HeapBufferedAsyncResponseConsumer consumer = new HeapBufferedAsyncResponseConsumer(TEST_BUFFER_LIMIT);
        bufferLimitTest(consumer, TEST_BUFFER_LIMIT);
    }

    public void testConfiguredBufferLimit() throws Exception {
        try {
            new HeapBufferedAsyncResponseConsumer(randomIntBetween(Integer.MIN_VALUE, 0));
        } catch(IllegalArgumentException e) {
            assertEquals("bufferLimit must be greater than 0", e.getMessage());
        }
        try {
            new HeapBufferedAsyncResponseConsumer(0);
        } catch(IllegalArgumentException e) {
            assertEquals("bufferLimit must be greater than 0", e.getMessage());
        }
        int bufferLimit = randomIntBetween(1, MAX_TEST_BUFFER_SIZE - 100);
        HeapBufferedAsyncResponseConsumer consumer = new HeapBufferedAsyncResponseConsumer(bufferLimit);
        bufferLimitTest(consumer, bufferLimit);
    }

    public void testCanConfigureHeapBufferLimitFromOutsidePackage() throws ClassNotFoundException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        int bufferLimit = randomIntBetween(1, Integer.MAX_VALUE);
        //we use reflection to make sure that the class can be instantiated from the outside, and the constructor is public
        Constructor<?> constructor =
                HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.class.getConstructor(Integer.TYPE);
        assertEquals(Modifier.PUBLIC, constructor.getModifiers() & Modifier.PUBLIC);
        Object object = constructor.newInstance(bufferLimit);
        assertThat(object, instanceOf(HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.class));
        HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory =
                (HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory) object;
        HttpAsyncResponseConsumer<HttpResponse> consumer = consumerFactory.createHttpAsyncResponseConsumer();
        assertThat(consumer, instanceOf(HeapBufferedAsyncResponseConsumer.class));
        HeapBufferedAsyncResponseConsumer bufferedAsyncResponseConsumer = (HeapBufferedAsyncResponseConsumer) consumer;
        assertEquals(bufferLimit, bufferedAsyncResponseConsumer.getBufferLimit());
    }

    public void testHttpAsyncResponseConsumerFactoryVisibility() throws ClassNotFoundException {
        assertEquals(Modifier.PUBLIC, HttpAsyncResponseConsumerFactory.class.getModifiers() & Modifier.PUBLIC);
    }

    private static void bufferLimitTest(HeapBufferedAsyncResponseConsumer consumer, int bufferLimit) throws Exception {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "OK");
        consumer.onResponseReceived(new BasicHttpResponse(statusLine));

        final AtomicReference<Long> contentLength = new AtomicReference<>();
        HttpEntity entity = new StringEntity("", ContentType.APPLICATION_JSON) {
            @Override
            public long getContentLength() {
                return contentLength.get();
            }
        };
        contentLength.set(randomLongBetween(0L, bufferLimit));
        consumer.onEntityEnclosed(entity, ContentType.APPLICATION_JSON);

        contentLength.set(randomLongBetween(bufferLimit + 1, MAX_TEST_BUFFER_SIZE));
        try {
            consumer.onEntityEnclosed(entity, ContentType.APPLICATION_JSON);
        } catch(ContentTooLongException e) {
            assertEquals("entity content is too long [" + entity.getContentLength() +
                    "] for the configured buffer limit [" + bufferLimit + "]", e.getMessage());
        }
    }
}
