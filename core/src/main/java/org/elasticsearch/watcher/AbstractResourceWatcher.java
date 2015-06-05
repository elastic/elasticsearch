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
package org.elasticsearch.watcher;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract resource watcher framework, which handles adding and removing listeners
 * and calling resource observer.
 */
public abstract class AbstractResourceWatcher<Listener> implements ResourceWatcher {
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private boolean initialized = false;

    @Override
    public void init() throws IOException {
        if (!initialized) {
            doInit();
            initialized = true;
        }
    }

    @Override
    public void checkAndNotify() throws IOException {
        init();
        doCheckAndNotify();
    }

    /**
     * Registers new listener
     */
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    /**
     * Unregisters a listener
     */
    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    /**
     * Returns a list of listeners
     */
    protected List<Listener> listeners() {
        return listeners;
    }

    /**
     * Will be called once on initialization
     */
    protected abstract void doInit() throws IOException;

    /**
     * Will be called periodically
     * <p/>
     * Implementing watcher should check resource and notify all {@link #listeners()}.
     */
    protected abstract void doCheckAndNotify() throws IOException;

}
