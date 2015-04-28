/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;


import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.trigger.TriggerService;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchLockService;
import org.elasticsearch.watcher.watch.WatchStore;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class WatcherService extends AbstractComponent {

    private final TriggerService triggerService;
    private final Watch.Parser watchParser;
    private final WatchStore watchStore;
    private final WatchLockService watchLockService;
    private final ExecutionService executionService;
    private final AtomicReference<State> state = new AtomicReference<>(State.STOPPED);

    @Inject
    public WatcherService(Settings settings, TriggerService triggerService, WatchStore watchStore, Watch.Parser watchParser,
                          ExecutionService executionService, WatchLockService watchLockService) {
        super(settings);
        this.triggerService = triggerService;
        this.watchStore = watchStore;
        this.watchParser = watchParser;
        this.watchLockService = watchLockService;
        this.executionService = executionService;
    }

    public void start(ClusterState clusterState) {
        if (state.compareAndSet(State.STOPPED, State.STARTING)) {
            logger.info("starting watch service...");
            watchLockService.start();

            // Try to load watch store before the execution service, b/c action depends on watch store
            watchStore.start(clusterState);
            executionService.start(clusterState);
            triggerService.start(watchStore.watches().values());
            state.set(State.STARTED);
            logger.info("watch service has started");
        }
    }

    public boolean validate(ClusterState state) {
        return watchStore.validate(state) && executionService.validate(state);
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            logger.info("stopping watch service...");
            triggerService.stop();
            executionService.stop();
            try {
                watchLockService.stop();
            } catch (WatchLockService.TimedOutException we) {
                logger.warn("error stopping WatchLockService", we);
            }
            watchStore.stop();
            state.set(State.STOPPED);
            logger.info("watch service has stopped");
        }
    }

    public WatchStore.WatchDelete deleteWatch(String name) throws InterruptedException, ExecutionException {
        ensureStarted();
        WatchLockService.Lock lock = watchLockService.acquire(name);
        try {
            WatchStore.WatchDelete delete = watchStore.delete(name);
            if (delete.deleteResponse().isFound()) {
                triggerService.remove(name);
            }
            return delete;
        } finally {
            lock.release();
        }
    }

    public IndexResponse putWatch(String id, BytesReference watchSource) {
        ensureStarted();
        WatchLockService.Lock lock = watchLockService.acquire(id);
        try {
            Watch watch = watchParser.parseWithSecrets(id, false, watchSource);
            WatchStore.WatchPut result = watchStore.put(watch);
            if (result.previous() == null || !result.previous().trigger().equals(result.current().trigger())) {
                triggerService.add(result.current());
            }
            return result.indexResponse();
        } catch (Exception e) {
            logger.warn("failed to put watch [{}]", e, id);
            throw new WatcherException("failed to put watch [{}]", e, id);
        } finally {
            lock.release();
        }
    }

    /**
     * TODO: add version, fields, etc support that the core get api has as well.
     */
    public Watch getWatch(String name) {
        return watchStore.get(name);
    }

    public State state() {
        return state.get();
    }

    /**
     * Acks the watch if needed
     */
    public Watch.Status ackWatch(String name) {
        ensureStarted();
        WatchLockService.Lock lock = watchLockService.acquire(name);
        try {
            Watch watch = watchStore.get(name);
            if (watch == null) {
                throw new WatcherException("watch [" + name + "] does not exist");
            }
            if (watch.ack()) {
                try {
                    watchStore.updateStatus(watch);
                } catch (IOException ioe) {
                    throw new WatcherException("failed to update the watch on ack", ioe);
                }
            }
            // we need to create a safe copy of the status
            return new Watch.Status(watch.status());
        } finally {
            lock.release();
        }
    }

    public long watchesCount() {
        return watchStore.watches().size();
    }

    private void ensureStarted() {
        if (state.get() != State.STARTED) {
            throw new ElasticsearchIllegalStateException("not started");
        }
    }

    /**
     * Encapsulates the state of the watcher plugin.
     */
    public static enum State {

        /**
         * The watcher plugin is not running and not functional.
         */
        STOPPED(0),

        /**
         * The watcher plugin is performing the necessary operations to get into a started state.
         */
        STARTING(1),

        /**
         * The watcher plugin is running and completely functional.
         */
        STARTED(2),

        /**
         * The watcher plugin is shutting down and not functional.
         */
        STOPPING(3);

        private final byte id;

        State(int id) {
            this.id = (byte) id;
        }

        public byte getId() {
            return id;
        }

        public static State fromId(byte id) {
            switch (id) {
                case 0:
                    return STOPPED;
                case 1:
                    return STARTING;
                case 2:
                    return STARTED;
                case 3:
                    return STOPPING;
                default:
                    throw new WatcherException("unknown watch service state id [" + id + "]");
            }
        }
    }
}
