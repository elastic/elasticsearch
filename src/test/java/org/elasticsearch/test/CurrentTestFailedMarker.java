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

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link RunListener} that detects  test failures. We need it because we need
 * to reset the global / suite level cluster if a test fails but don't wanna reset it
 * for every subsequent test.
 */
public class CurrentTestFailedMarker extends RunListener {

    private static final AtomicBoolean failed = new AtomicBoolean(false);

    @Override
    public void testFailure(Failure failure) throws Exception {
        failed.set(true);
    }

    @Override
    public void testRunStarted(Description description) throws Exception {
        failed.set(false);
    }

    /**
     * Returns <code>true</code> iff the previously executed test failed. Otherwise <code>false</code>
     */
    public static boolean testFailed() {
        return failed.get();
    }
}
