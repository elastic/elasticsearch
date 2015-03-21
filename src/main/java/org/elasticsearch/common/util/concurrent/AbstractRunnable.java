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

/**
 * An extension to runnable.
 */
public abstract class AbstractRunnable implements Runnable {

    /**
     * Should the runnable force its execution in case it gets rejected?
     */
    public boolean isForceExecution() {
        return false;
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (InterruptedException ex) {
            Thread.interrupted();
            onFailure(ex);
        } catch (Throwable t) {
            onFailure(t);
        } finally {
            onAfter();
        }
    }

    /**
     * This method is called in a finally block after successful execution
     * or on a rejection.
     */
    public void onAfter() {
        // nothing by default
    }

    /**
     * This method is invoked for all exception thrown by {@link #doRun()}
     */
    public abstract void onFailure(Throwable t);

    /**
     * This should be executed if the thread-pool executing this action rejected the execution.
     * The default implementation forwards to {@link #onFailure(Throwable)}
     */
    public void onRejection(Throwable t) {
        onFailure(t);
    }

    /**
     * This method has the same semantics as {@link Runnable#run()}
     * @throws InterruptedException if the run method throws an InterruptedException
     */
    protected abstract void doRun() throws Exception;
}
