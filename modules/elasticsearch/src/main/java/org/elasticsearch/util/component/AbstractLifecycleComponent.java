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

package org.elasticsearch.util.component;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.util.settings.Settings;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractLifecycleComponent<T> extends AbstractComponent implements LifecycleComponent<T> {

    protected final Lifecycle lifecycle = new Lifecycle();

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<LifecycleListener>();

    protected AbstractLifecycleComponent(Settings settings) {
        super(settings);
    }

    protected AbstractLifecycleComponent(Settings settings, Class customClass) {
        super(settings, customClass);
    }

    protected AbstractLifecycleComponent(Settings settings, Class loggerClass, Class componentClass) {
        super(settings, loggerClass, componentClass);
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public void addLifecycleListener(LifecycleListener listener) {
        listeners.add(listener);
    }

    @Override public void removeLifecycleListener(LifecycleListener listener) {
        listeners.remove(listener);
    }

    @SuppressWarnings({"unchecked"}) @Override public T start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return (T) this;
        }
        for (LifecycleListener listener : listeners) {
            listener.beforeStart();
        }
        doStart();
        for (LifecycleListener listener : listeners) {
            listener.afterStart();
        }
        return (T) this;
    }

    protected abstract void doStart() throws ElasticSearchException;

    @SuppressWarnings({"unchecked"}) @Override public T stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return (T) this;
        }
        for (LifecycleListener listener : listeners) {
            listener.beforeStop();
        }
        doStop();
        for (LifecycleListener listener : listeners) {
            listener.afterStop();
        }
        return (T) this;
    }

    protected abstract void doStop() throws ElasticSearchException;

    @Override public void close() throws ElasticSearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        for (LifecycleListener listener : listeners) {
            listener.beforeClose();
        }
        doClose();
        for (LifecycleListener listener : listeners) {
            listener.afterClose();
        }
    }

    protected abstract void doClose() throws ElasticSearchException;
}
