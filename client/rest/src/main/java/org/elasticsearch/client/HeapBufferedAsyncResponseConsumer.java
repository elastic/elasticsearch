/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
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

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.ContentBufferEntity;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.util.ByteBufferAllocator;
import org.apache.http.nio.util.HeapByteBufferAllocator;
import org.apache.http.nio.util.SimpleInputBuffer;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

/**
 * Default implementation of {@link org.apache.http.nio.protocol.HttpAsyncResponseConsumer}. Buffers the whole
 * response content in heap memory, meaning that the size of the buffer is equal to the content-length of the response.
 * Limits the size of responses that can be read based on a configurable argument. Throws an exception in case the entity is longer
 * than the configured buffer limit.
 */
public class HeapBufferedAsyncResponseConsumer extends AbstractAsyncResponseConsumer<HttpResponse> {

    private final int bufferLimitBytes;
    private volatile HttpResponse response;
    private volatile SimpleInputBuffer buf;

    /**
     * Creates a new instance of this consumer with the provided buffer limit
     */
    public HeapBufferedAsyncResponseConsumer(int bufferLimit) {
        if (bufferLimit <= 0) {
            throw new IllegalArgumentException("bufferLimit must be greater than 0");
        }
        this.bufferLimitBytes = bufferLimit;
    }

    /**
     * Get the limit of the buffer.
     */
    public int getBufferLimit() {
        return bufferLimitBytes;
    }

    @Override
    protected void onResponseReceived(HttpResponse httpResponse) throws HttpException, IOException {
        this.response = httpResponse;
    }

    @Override
    protected void onEntityEnclosed(HttpEntity entity, ContentType contentType) throws IOException {
        long len = entity.getContentLength();
        if (len > bufferLimitBytes) {
            throw new ContentTooLongException("entity content is too long [" + len +
                    "] for the configured buffer limit [" + bufferLimitBytes + "]");
        }
        if (len < 0) {
            len = 4096;
        }
        this.buf = new SimpleInputBuffer((int) len, getByteBufferAllocator());
        this.response.setEntity(new ContentBufferEntity(entity, this.buf));
    }

    /**
     * Returns the instance of {@link ByteBufferAllocator} to use for content buffering.
     * Allows to plug in any {@link ByteBufferAllocator} implementation.
     */
    protected ByteBufferAllocator getByteBufferAllocator() {
        return HeapByteBufferAllocator.INSTANCE;
    }

    @Override
    protected void onContentReceived(ContentDecoder decoder, IOControl ioctrl) throws IOException {
        this.buf.consumeContent(decoder);
    }

    @Override
    protected HttpResponse buildResult(HttpContext context) throws Exception {
        return response;
    }

    @Override
    protected void releaseResources() {
        response = null;
    }
}
