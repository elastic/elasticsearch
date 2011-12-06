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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public abstract class AdapterActionFuture<T, L> extends BaseFuture<T> implements ActionFuture<T>, ActionListener<L> {

    private Throwable rootFailure;

    @Override
    public T actionGet() throws ElasticSearchException {
        try {
            return get();
        } catch (InterruptedException e) {
            throw new ElasticSearchInterruptedException(e.getMessage());
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    @Override
    public T actionGet(String timeout) throws ElasticSearchException {
        return actionGet(TimeValue.parseTimeValue(timeout, null));
    }

    @Override
    public T actionGet(long timeoutMillis) throws ElasticSearchException {
        return actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(TimeValue timeout) throws ElasticSearchException {
        return actionGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(long timeout, TimeUnit unit) throws ElasticSearchException {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ElasticSearchTimeoutException(e.getMessage());
        } catch (InterruptedException e) {
            throw new ElasticSearchInterruptedException(e.getMessage());
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    static ElasticSearchException rethrowExecutionException(ExecutionException e) {
        if (e.getCause() instanceof ElasticSearchException) {
            ElasticSearchException esEx = (ElasticSearchException) e.getCause();
            Throwable root = esEx.unwrapCause();
            if (root instanceof ElasticSearchException) {
                return (ElasticSearchException) root;
            }
            return new UncategorizedExecutionException("Failed execution", root);
        } else {
            return new UncategorizedExecutionException("Failed execution", e);
        }
    }

    @Override
    public void onResponse(L result) {
        set(convert(result));
    }

    @Override
    public void onFailure(Throwable e) {
        setException(e);
    }

    protected abstract T convert(L listenerResponse);

    @Override
    public Throwable getRootFailure() {
        return rootFailure;
    }
}
