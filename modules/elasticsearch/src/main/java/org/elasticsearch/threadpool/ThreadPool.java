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

package org.elasticsearch.threadpool;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.DynamicExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.MoreExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;

import java.util.Map;
import java.util.concurrent.*;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class ThreadPool extends AbstractComponent {

    public static class Names {
        public static final String SAME = "same";
        public static final String CACHED = "cached";
        public static final String INDEX = "index";
        public static final String SEARCH = "search";
        public static final String PERCOLATE = "percolate";
        public static final String MANAGEMENT = "management";
        public static final String MERGE = "merge";
        public static final String SNAPSHOT = "snapshot";
    }

    private final ImmutableMap<String, Executor> executors;

    private final ScheduledExecutorService scheduler;

    private final EstimatedTimeThread estimatedTimeThread;

    public ThreadPool() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    @Inject public ThreadPool(Settings settings) {
        super(settings);

        Map<String, Settings> groupSettings = settings.getGroups("threadpool");

        Map<String, Executor> executors = Maps.newHashMap();
        executors.put(Names.CACHED, build(Names.CACHED, "cached", groupSettings.get(Names.CACHED), settingsBuilder().put("keep_alive", "30s").build()));
        executors.put(Names.INDEX, build(Names.INDEX, "cached", groupSettings.get(Names.INDEX), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.SEARCH, build(Names.SEARCH, "cached", groupSettings.get(Names.SEARCH), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.PERCOLATE, build(Names.PERCOLATE, "cached", groupSettings.get(Names.PERCOLATE), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.MANAGEMENT, build(Names.MANAGEMENT, "scaling", groupSettings.get(Names.MANAGEMENT), settingsBuilder().put("keep_alive", "30s").put("size", 20).build()));
        executors.put(Names.MERGE, build(Names.MERGE, "scaling", groupSettings.get(Names.MERGE), settingsBuilder().put("keep_alive", "30s").put("size", 20).build()));
        executors.put(Names.SNAPSHOT, build(Names.SNAPSHOT, "scaling", groupSettings.get(Names.SNAPSHOT), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.SAME, MoreExecutors.sameThreadExecutor());
        this.executors = ImmutableMap.copyOf(executors);
        this.scheduler = Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory(settings, "[scheduler]"));

        TimeValue estimatedTimeInterval = componentSettings.getAsTime("estimated_time_interval", TimeValue.timeValueMillis(200));
        this.estimatedTimeThread = new EstimatedTimeThread(EsExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.estimatedTimeThread.start();
    }

    public long estimatedTimeInMillis() {
        return estimatedTimeThread.estimatedTimeInMillis();
    }

    public Executor cached() {
        return executor(Names.CACHED);
    }

    public Executor executor(String name) {
        Executor executor = executors.get(name);
        if (executor == null) {
            throw new ElasticSearchIllegalArgumentException("No executor found for [" + name + "]");
        }
        return executor;
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, TimeValue interval) {
        return scheduler.scheduleWithFixedDelay(new LoggingRunnable(command), interval.millis(), interval.millis(), TimeUnit.MILLISECONDS);
    }

    public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
        if (!Names.SAME.equals(name)) {
            command = new ThreadedRunnable(command, executor(name));
        }
        return scheduler.schedule(command, delay.millis(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        estimatedTimeThread.running = false;
        estimatedTimeThread.interrupt();
        scheduler.shutdown();
        for (Executor executor : executors.values()) {
            if (executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).shutdown();
            }
        }
    }

    public void shutdownNow() {
        estimatedTimeThread.running = false;
        estimatedTimeThread.interrupt();
        scheduler.shutdownNow();
        for (Executor executor : executors.values()) {
            if (executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).shutdownNow();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (Executor executor : executors.values()) {
            if (executor instanceof ThreadPoolExecutor) {
                result &= ((ThreadPoolExecutor) executor).awaitTermination(timeout, unit);
            }
        }
        return result;
    }

    private Executor build(String name, String defaultType, @Nullable Settings settings, Settings defaultSettings) {
        if (settings == null) {
            settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        }
        String type = settings.get("type", defaultType);
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[" + name + "]");
        if ("same".equals(type)) {
            logger.debug("creating thread_pool [{}], type [{}]", name, type);
            return MoreExecutors.sameThreadExecutor();
        } else if ("cached".equals(type)) {
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultSettings.getAsTime("keep_alive", timeValueMinutes(5)));
            logger.debug("creating thread_pool [{}], type [{}], keep_alive [{}]", name, type, keepAlive);
            return new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                    keepAlive.millis(), TimeUnit.MILLISECONDS,
                    new SynchronousQueue<Runnable>(),
                    threadFactory);
        } else if ("fixed".equals(type)) {
            int size = settings.getAsInt("size", defaultSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5));
            logger.debug("creating thread_pool [{}], type [{}], size [{}]", name, type, size);
            return new ThreadPoolExecutor(size, size,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedTransferQueue<Runnable>(),
                    threadFactory);
        } else if ("scaling".equals(type)) {
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultSettings.getAsTime("keep_alive", timeValueMinutes(5)));
            int min = settings.getAsInt("min", defaultSettings.getAsInt("min", 1));
            int size = settings.getAsInt("size", defaultSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5));
            logger.debug("creating thread_pool [{}], type [{}], min [{}], size [{}], keep_alive [{}]", name, type, min, size, keepAlive);
            return DynamicExecutors.newScalingThreadPool(min, size, keepAlive.millis(), threadFactory);
        } else if ("blocking".equals(type)) {
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultSettings.getAsTime("keep_alive", timeValueMinutes(5)));
            int min = settings.getAsInt("min", defaultSettings.getAsInt("min", 1));
            int size = settings.getAsInt("size", defaultSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5));
            SizeValue capacity = settings.getAsSize("capacity", defaultSettings.getAsSize("capacity", new SizeValue(0)));
            TimeValue waitTime = settings.getAsTime("wait_time", defaultSettings.getAsTime("wait_time", timeValueSeconds(60)));
            logger.debug("creating thread_pool [{}], type [{}], min [{}], size [{}], keep_alive [{}], wait_time [{}]", name, type, min, size, keepAlive, waitTime);
            return DynamicExecutors.newBlockingThreadPool(min, size, keepAlive.millis(), (int) capacity.singles(), waitTime.millis(), threadFactory);
        }
        throw new ElasticSearchIllegalArgumentException("No type found [" + type + "], for [" + name + "]");
    }

    class LoggingRunnable implements Runnable {

        private final Runnable runnable;

        LoggingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override public void run() {
            try {
                runnable.run();
            } catch (Exception e) {
                logger.warn("failed to run {}", e, runnable.toString());
            }
        }

        @Override public int hashCode() {
            return runnable.hashCode();
        }

        @Override public boolean equals(Object obj) {
            return runnable.equals(obj);
        }

        @Override public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    class ThreadedRunnable implements Runnable {

        private final Runnable runnable;

        private final Executor executor;

        ThreadedRunnable(Runnable runnable, Executor executor) {
            this.runnable = runnable;
            this.executor = executor;
        }

        @Override public void run() {
            executor.execute(runnable);
        }

        @Override public int hashCode() {
            return runnable.hashCode();
        }

        @Override public boolean equals(Object obj) {
            return runnable.equals(obj);
        }

        @Override public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    static class EstimatedTimeThread extends Thread {

        final long interval;

        volatile boolean running = true;

        volatile long estimatedTimeInMillis;

        EstimatedTimeThread(String name, long interval) {
            super(name);
            this.interval = interval;
            setDaemon(true);
        }

        public long estimatedTimeInMillis() {
            return this.estimatedTimeInMillis;
        }

        @Override public void run() {
            while (running) {
                estimatedTimeInMillis = System.currentTimeMillis();
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    running = false;
                    return;
                }
            }
        }
    }
}
