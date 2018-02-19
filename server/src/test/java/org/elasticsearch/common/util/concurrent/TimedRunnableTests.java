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

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public final class TimedRunnableTests extends ESTestCase {

    public void testTimedRunnableDelegatesToAbstractRunnable() {
        final boolean isForceExecution = randomBoolean();
        final AtomicBoolean onAfter = new AtomicBoolean();
        final AtomicReference<Exception> onRejection = new AtomicReference<>();
        final AtomicReference<Exception> onFailure = new AtomicReference<>();
        final AtomicBoolean doRun = new AtomicBoolean();

        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return isForceExecution;
            }

            @Override
            public void onAfter() {
                onAfter.set(true);
            }

            @Override
            public void onRejection(final Exception e) {
                onRejection.set(e);
            }

            @Override
            public void onFailure(final Exception e) {
                onFailure.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                doRun.set(true);
            }
        };

        final TimedRunnable timedRunnable = new TimedRunnable(runnable);

        assertThat(timedRunnable.isForceExecution(), equalTo(isForceExecution));

        timedRunnable.onAfter();
        assertTrue(onAfter.get());

        final Exception rejection = new RejectedExecutionException();
        timedRunnable.onRejection(rejection);
        assertThat(onRejection.get(), equalTo(rejection));

        final Exception failure = new Exception();
        timedRunnable.onFailure(failure);
        assertThat(onFailure.get(), equalTo(failure));

        timedRunnable.run();
        assertTrue(doRun.get());
    }

    public void testTimedRunnableDelegatesRunInFailureCase() {
        final AtomicBoolean onAfter = new AtomicBoolean();
        final AtomicReference<Exception> onFailure = new AtomicReference<>();
        final AtomicBoolean doRun = new AtomicBoolean();

        final Exception exception = new Exception();

        final AbstractRunnable runnable = new AbstractRunnable() {
            @Override
            public void onAfter() {
                onAfter.set(true);
            }

            @Override
            public void onFailure(final Exception e) {
                onFailure.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                doRun.set(true);
                throw exception;
            }
        };

        final TimedRunnable timedRunnable = new TimedRunnable(runnable);
        timedRunnable.run();
        assertTrue(doRun.get());
        assertThat(onFailure.get(), equalTo(exception));
        assertTrue(onAfter.get());
    }

}
