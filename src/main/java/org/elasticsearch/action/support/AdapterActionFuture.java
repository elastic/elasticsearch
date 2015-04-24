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

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchTimeoutException;
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
    public T actionGet() throws ElasticsearchException {
        try {
            return get();
        } catch (InterruptedException e) {
            throw new ElasticsearchIllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    @Override
    public T actionGet(String timeout) throws ElasticsearchException {
        return actionGet(TimeValue.parseTimeValue(timeout, null));
    }

    @Override
    public T actionGet(long timeoutMillis) throws ElasticsearchException {
        return actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(TimeValue timeout) throws ElasticsearchException {
        return actionGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(long timeout, TimeUnit unit) throws ElasticsearchException {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            throw new ElasticsearchTimeoutException(e.getMessage());
        } catch (InterruptedException e) {
            throw new ElasticsearchIllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw rethrowExecutionException(e);
        }
    }

    static ElasticsearchException rethrowExecutionException(ExecutionException e) {
        if (e.getCause() instanceof ElasticsearchException) {
            ElasticsearchException esEx = (ElasticsearchException) e.getCause();
            Throwable root = esEx.unwrapCause();
            if (root instanceof ElasticsearchException) {
                return (ElasticsearchException) root;
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
