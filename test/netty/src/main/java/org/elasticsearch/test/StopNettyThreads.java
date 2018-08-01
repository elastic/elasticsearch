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

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.rules.ExternalResource;

/**
 * A JUnit class level rule that runs after the AfterClass method in Elasticsearch test cases,
 * which stops the cluster. After the cluster is stopped, there are a few netty threads that
 * can linger, so we wait for them to finish otherwise these lingering threads can intermittently
 * trigger the thread leak detector
 */
public final class StopNettyThreads extends ExternalResource {

    @Override
    protected void after() {
        try {
            GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IllegalStateException e) {
            if (e.getMessage().equals("thread was not started") == false) {
                throw e;
            }
            // ignore since the thread was never started
        }

        try {
            ThreadDeathWatcher.awaitInactivity(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
