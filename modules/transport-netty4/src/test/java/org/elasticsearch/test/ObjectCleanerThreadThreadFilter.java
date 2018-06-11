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

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The Netty object cleaner thread is not closeable and it does not terminate in a timely manner. This means that thread leak control in
 * tests will fail test suites when the object cleaner thread has not terminated. Since there is not a reliable way to terminate this thread
 * we instead filter it out of thread leak control.
 */
public class ObjectCleanerThreadThreadFilter implements ThreadFilter {

    @Override
    public boolean reject(final Thread t) {
        // TODO: replace with constant from Netty when https://github.com/netty/netty/pull/8014 is integrated
        return "ObjectCleanerThread".equals(t.getName());
    }

}

