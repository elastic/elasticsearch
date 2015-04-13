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
package org.elasticsearch.test;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link org.junit.rules.TestRule} that detects test failures and allows to run an arbitrary task after a test failed.
 * Allows also to run an arbitrary task in any case, regardless of the test result.
 * It is possible to force running the first arbitrary task from the outside, as if the test was failed, when e.g. it needs
 * to be performed based on external events.
 *
 * We need it to be able to reset the suite level cluster after each failure, or if there is a problem
 * during the after test operations.
 */
public class AfterTestRule extends TestWatcher {

    private final AtomicBoolean failed = new AtomicBoolean(false);

    private final Task task;

    public AfterTestRule(Task task) {
        this.task = task;
    }

    void forceFailure() {
        failed.set(true);
    }

    @Override
    protected void failed(Throwable e, Description description) {
        failed.set(true);
    }

    @Override
    protected void finished(Description description) {
        if (failed.compareAndSet(true, false)) {
            task.onTestFailed();
        }
        task.onTestFinished();
    }

    /**
     * Task to be executed after each test if required, no-op by default
     */
    public static class Task {
        /**
         * The task that needs to be executed after a test fails
         */
        void onTestFailed() {

        }

        /**
         * The task that needs to be executed when a test is completed, regardless of its result
         */
        void onTestFinished() {

        }
    }
}
