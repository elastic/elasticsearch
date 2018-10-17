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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;

import java.util.concurrent.ExecutorService;

/**
 * A future implementation that allows for the result to be passed to listeners waiting for
 * notification. This is useful for cases where a computation is requested many times
 * concurrently, but really only needs to be performed a single time. Once the computation
 * has been performed the registered listeners will be notified by submitting a runnable
 * for execution in the provided {@link ExecutorService}. If the computation has already
 * been performed, a request to add a listener will simply result in execution of the listener
 * on the calling thread.
 */
public final class ListenableFuture<V> extends BaseFuture<V> implements ActionListener<V> {

    /**
     * Adds a listener to this future. If the future has not yet completed, the listener will be
     * notified of a response or exception in a runnable submitted to the ExecutorService provided.
     * If the future has completed, the listener will be notified immediately without forking to
     * a different thread.
     */
    public void addListener(ActionListener<V> listener, ExecutorService executor, ThreadContext threadContext) {
        ContextPreservingActionListener<V> wrappedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        whenCompleteAsync((val, throwable) -> {
            if (throwable == null) {
                try {
                    wrappedListener.onResponse(val);
                } catch (Exception e) {
                    wrappedListener.onFailure(e);
                }
            } else {
                assert throwable instanceof Exception : "Expected exception but was: " + throwable.getClass();
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                wrappedListener.onFailure((Exception) throwable);
            }
        }, executor);
    }

    @Override
    public void onResponse(V v) {
        final boolean set = complete(v);
        if (set == false) {
            throw new IllegalStateException("did not set value, value or exception already set?");
        }
    }

    @Override
    public void onFailure(Exception e) {
        final boolean set = completeExceptionally(e);
        if (set == false) {
            throw new IllegalStateException("did not set exception, value already set or exception already set?");
        }
    }
}
