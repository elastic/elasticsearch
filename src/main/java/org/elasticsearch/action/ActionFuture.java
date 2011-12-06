/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An extension to {@link Future} allowing for simplified "get" operations.
 *
 *
 */
public interface ActionFuture<T> extends Future<T> {

    /**
     * Similar to {@link #get()}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     * <p/>
     * <p>Note, the actual cause is unwrapped to the actual failure (for example, unwrapped
     * from {@link org.elasticsearch.transport.RemoteTransportException}. The root failure is
     * still accessible using {@link #getRootFailure()}.
     */
    T actionGet() throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     * <p/>
     * <p>Note, the actual cause is unwrapped to the actual failure (for example, unwrapped
     * from {@link org.elasticsearch.transport.RemoteTransportException}. The root failure is
     * still accessible using {@link #getRootFailure()}.
     */
    T actionGet(String timeout) throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     * <p/>
     * <p>Note, the actual cause is unwrapped to the actual failure (for example, unwrapped
     * from {@link org.elasticsearch.transport.RemoteTransportException}. The root failure is
     * still accessible using {@link #getRootFailure()}.
     *
     * @param timeoutMillis Timeout in millis
     */
    T actionGet(long timeoutMillis) throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     * <p/>
     * <p>Note, the actual cause is unwrapped to the actual failure (for example, unwrapped
     * from {@link org.elasticsearch.transport.RemoteTransportException}. The root failure is
     * still accessible using {@link #getRootFailure()}.
     */
    T actionGet(long timeout, TimeUnit unit) throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     * <p/>
     * <p>Note, the actual cause is unwrapped to the actual failure (for example, unwrapped
     * from {@link org.elasticsearch.transport.RemoteTransportException}. The root failure is
     * still accessible using {@link #getRootFailure()}.
     */
    T actionGet(TimeValue timeout) throws ElasticSearchException;

    /**
     * The root (possibly) wrapped failure.
     */
    @Nullable
    Throwable getRootFailure();
}
