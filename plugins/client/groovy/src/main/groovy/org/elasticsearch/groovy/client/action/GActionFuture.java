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

package org.elasticsearch.groovy.client.action;

import groovy.lang.Closure;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * @author kimchy (shay.banon)
 */
public class GActionFuture<T> extends PlainListenableActionFuture<T> {

    protected GActionFuture(ThreadPool threadPool, ActionRequest request) {
        super(request.listenerThreaded(), threadPool);
    }

    public void setListener(final Closure listener) {
        addListener(new ActionListener<T>() {
            @Override public void onResponse(T t) {
                listener.call(this);
            }

            @Override public void onFailure(Throwable e) {
                listener.call(this);
            }
        });
    }

    public void setSuccess(final Closure success) {
        addListener(new ActionListener<T>() {
            @Override public void onResponse(T t) {
                success.call(t);
            }

            @Override public void onFailure(Throwable e) {
                // ignore
            }
        });
    }

    public void setFailure(final Closure failure) {
        addListener(new ActionListener<T>() {
            @Override public void onResponse(T t) {
                // nothing
            }

            @Override public void onFailure(Throwable e) {
                failure.call(e);
            }
        });
    }

    public T getResponse() {
        return actionGet();
    }

    public T response(String timeout) throws ElasticSearchException {
        return super.actionGet(timeout);
    }

    public T response(long timeoutMillis) throws ElasticSearchException {
        return super.actionGet(timeoutMillis);
    }

    public T response(TimeValue timeout) throws ElasticSearchException {
        return super.actionGet(timeout);
    }

    public T response(long timeout, TimeUnit unit) throws ElasticSearchException {
        return super.actionGet(timeout, unit);
    }
}
