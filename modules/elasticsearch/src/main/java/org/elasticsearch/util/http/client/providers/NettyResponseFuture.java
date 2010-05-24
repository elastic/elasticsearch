/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.http.client.providers;

import org.elasticsearch.util.http.client.AsyncHandler;
import org.elasticsearch.util.http.client.FutureImpl;
import org.elasticsearch.util.http.client.Request;
import org.elasticsearch.util.http.url.Url;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.net.MalformedURLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Future} that can be used to track when an asynchronous HTTP request has been fully processed.
 *
 * @param <V>
 */
public final class NettyResponseFuture<V> implements FutureImpl<V> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final AtomicBoolean isCancelled = new AtomicBoolean(false);
    private final AsyncHandler<V> asyncHandler;
    private final int responseTimeoutInMs;
    private final Request request;
    private final HttpRequest nettyRequest;
    private final AtomicReference<V> content = new AtomicReference<V>();
    private Url url;
    private boolean keepAlive = true;
    private HttpResponse httpResponse;
    private final AtomicReference<ExecutionException> exEx = new AtomicReference<ExecutionException>();
    private final AtomicInteger redirectCount = new AtomicInteger();
    private Future<Object> reaperFuture;

    public NettyResponseFuture(Url url,
                               Request request,
                               AsyncHandler<V> asyncHandler,
                               HttpRequest nettyRequest,
                               int responseTimeoutInMs) {

        this.asyncHandler = asyncHandler;
        this.responseTimeoutInMs = responseTimeoutInMs;
        this.request = request;
        this.nettyRequest = nettyRequest;
        this.url = url;
    }

    public Url getUrl() throws MalformedURLException {
        return url;
    }

    public void setUrl(Url url) {
        this.url = url;
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public boolean isDone() {
        return isDone.get();
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public boolean isCancelled() {
        return isCancelled.get();
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public boolean cancel(boolean force) {
        latch.countDown();
        isCancelled.set(true);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(responseTimeoutInMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public V get(long l, TimeUnit tu) throws InterruptedException, TimeoutException, ExecutionException {
        if (!isDone() && !isCancelled()) {
            if (!latch.await(l, tu)) {
                isCancelled.set(true);
                TimeoutException te = new TimeoutException("No response received");
                onThrowable(te);
                throw te;
            }
            isDone.set(true);

            if (exEx.get() != null) {
                throw exEx.getAndSet(null);
            }
        }
        return (V) getContent();
    }

    private void onThrowable(Throwable t) {
        asyncHandler.onThrowable(t);
    }

    V getContent() {
        if (content.get() == null) {
            try {
                content.set(asyncHandler.onCompleted());
            } catch (Throwable ex) {
                onThrowable(ex);
                throw new RuntimeException(ex);
            }
        }
        return content.get();
    }

    public final void done() {
        if (exEx.get() != null) {
            return;
        }
        if (reaperFuture != null) reaperFuture.cancel(true);
        isDone.set(true);
        getContent();
        latch.countDown();
    }

    public final void abort(final Throwable t) {
        if (isDone.get() || isCancelled.get()) return;

        if (reaperFuture != null) reaperFuture.cancel(true);

        if (exEx.get() == null) {
            exEx.set(new ExecutionException(t));
        }
        asyncHandler.onThrowable(t);
        isDone.set(true);
        latch.countDown();
    }

    public final Request getRequest() {
        return request;
    }

    public final HttpRequest getNettyRequest() {
        return nettyRequest;
    }

    public final AsyncHandler<V> getAsyncHandler() {
        return asyncHandler;
    }

    public final boolean getKeepAlive() {
        return keepAlive;
    }

    public final void setKeepAlive(final boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public final HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public final void setHttpResponse(final HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    public int incrementAndGetCurrentRedirectCount() {
        return redirectCount.incrementAndGet();
    }

    public void setReaperFuture(Future<Object> reaperFuture) {
        this.reaperFuture = reaperFuture;
    }
}
