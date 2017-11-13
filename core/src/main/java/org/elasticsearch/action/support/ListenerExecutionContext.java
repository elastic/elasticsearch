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

import org.elasticsearch.action.ActionListener;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

public class ListenerExecutionContext<V> implements ActionListener<V> {

    private static final Object NULL_VALUE = new Object();

    private final ConcurrentLinkedQueue<ActionListener<V>> listeners = new ConcurrentLinkedQueue<>();
    private final AtomicReference<Object> result = new AtomicReference<>(null);

    @Override
    public void onResponse(V value) {
        if (result.compareAndSet(null, value != null ? value : NULL_VALUE)) {
            ActionListener<V> listener;
            while ((listener = listeners.poll()) != null) {
                listener.onResponse(value);
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (e == null) {
            throw new IllegalArgumentException("Exception cannot be null");
        }
        if (result.compareAndSet(null, e)) {
            ActionListener<V> listener;
            while ((listener = listeners.poll()) != null) {
                listener.onFailure(e);
            }
        }
    }

    public boolean isDone() {
        return result.get() != null;
    }

    @SuppressWarnings("unchecked")
    public void addListener(ActionListener<V> listener) {
        listeners.offer(listener);

        Object result = this.result.get();
        if (result != null) {
            // if the result is not null this context has already been completed. To ensure that the listener
            // is executed, we attempt to remove the listener we just added from the queue. If we are
            // successful in removing the listener, the thread that completed this context will no longer
            // have access to the listener. In that case, we complete the listener ourselves. The
            // ConcurrentLinkedQueue will only allow one thread to successfully remove the listener. This
            // logic ensures that the listener is only completed once.
            if (listeners.remove(listener)) {
                if (result instanceof Exception) {
                    listener.onFailure((Exception) result);
                } else if (result == NULL_VALUE) {
                    listener.onResponse(null);
                } else {
                    listener.onResponse((V) result);
                }
            }
        }
    }
}
