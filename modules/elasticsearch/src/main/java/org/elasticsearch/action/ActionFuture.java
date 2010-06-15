/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An extension to {@link Future} allowing for simplified "get" operations.
 *
 * @author kimchy (shay.banon)
 */
public interface ActionFuture<T> extends Future<T> {

    /**
     * Similar to {@link #get()}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     */
    T actionGet() throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     */
    T actionGet(String timeout) throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     *
     * @param timeoutMillis Timeout in millis
     */
    T actionGet(long timeoutMillis) throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     */
    T actionGet(long timeout, TimeUnit unit) throws ElasticSearchException;

    /**
     * Similar to {@link #get(long, java.util.concurrent.TimeUnit)}, just wrapping the {@link InterruptedException} with
     * {@link org.elasticsearch.ElasticSearchInterruptedException}, and throwing the actual
     * cause of the {@link java.util.concurrent.ExecutionException}.
     */
    T actionGet(TimeValue timeout) throws ElasticSearchException;
}
