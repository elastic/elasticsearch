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

package org.elasticsearch.threadpool.support;

import org.elasticsearch.threadpool.FutureListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractThreadPool extends AbstractComponent implements ThreadPool {

    protected volatile boolean started;

    protected ExecutorService executorService;

    protected ScheduledExecutorService scheduledExecutorService;

    protected AbstractThreadPool(Settings settings) {
        super(settings);
    }

    public abstract String getType();

    @Override public boolean isStarted() {
        return started;
    }

    @Override public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutorService.schedule(command, delay, unit);
    }

    @Override public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return scheduledExecutorService.schedule(callable, delay, unit);
    }

    @Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override public void shutdown() {
        started = false;
        logger.debug("Shutting down {} thread pool", getType());
        executorService.shutdown();
        scheduledExecutorService.shutdown();
    }

    @Override public List<Runnable> shutdownNow() {
        started = false;
        List<Runnable> result = new ArrayList<Runnable>();
        result.addAll(executorService.shutdownNow());
        result.addAll(scheduledExecutorService.shutdownNow());
        return result;
    }

    @Override public boolean isShutdown() {
        return executorService.isShutdown() || scheduledExecutorService.isShutdown();
    }

    @Override public boolean isTerminated() {
        return executorService.isTerminated() || scheduledExecutorService.isTerminated();
    }

    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = executorService.awaitTermination(timeout, unit);
        result &= scheduledExecutorService.awaitTermination(timeout, unit);
        return result;
    }

    @Override public <T> Future<T> submit(Callable<T> task) {
        return executorService.submit(task);
    }

    @Override public <T> Future<T> submit(Callable<T> task, FutureListener<T> listener) {
        return executorService.submit(new FutureCallable<T>(task, listener));
    }

    @Override public <T> Future<T> submit(Runnable task, T result) {
        return executorService.submit(task, result);
    }

    @Override public <T> Future<T> submit(Runnable task, T result, FutureListener<T> listener) {
        return executorService.submit(new FutureRunnable<T>(task, result, listener), result);
    }

    @Override public Future<?> submit(Runnable task) {
        return executorService.submit(task);
    }

    @Override public Future<?> submit(Runnable task, FutureListener<?> listener) {
        return executorService.submit(new FutureRunnable(task, null, listener));
    }

    @Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, TimeValue interval) {
        return scheduleWithFixedDelay(command, interval.millis(), interval.millis(), TimeUnit.MILLISECONDS);
    }

    @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executorService.invokeAll(tasks);
    }

    @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executorService.invokeAll(tasks, timeout, unit);
    }

    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executorService.invokeAny(tasks);
    }

    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return executorService.invokeAny(tasks, timeout, unit);
    }

    @Override public void execute(Runnable command) {
        executorService.execute(command);
    }

    protected static class FutureCallable<T> implements Callable<T> {

        private final Callable<T> callable;

        private final FutureListener<T> listener;

        public FutureCallable(Callable<T> callable, FutureListener<T> listener) {
            this.callable = callable;
            this.listener = listener;
        }

        @Override public T call() throws Exception {
            try {
                T result = callable.call();
                listener.onResult(result);
                return result;
            } catch (Exception e) {
                listener.onException(e);
                throw e;
            }
        }
    }

    protected static class FutureRunnable<T> implements Runnable {

        private final Runnable runnable;

        private final T result;

        private final FutureListener<T> listener;

        private FutureRunnable(Runnable runnable, T result, FutureListener<T> listener) {
            this.runnable = runnable;
            this.result = result;
            this.listener = listener;
        }

        @Override public void run() {
            try {
                runnable.run();
                listener.onResult(result);
            } catch (Exception e) {
                listener.onException(e);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
            }
        }
    }
}
