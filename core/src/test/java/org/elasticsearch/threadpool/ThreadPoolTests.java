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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class ThreadPoolTests extends ESTestCase {

    public void testIngestThreadPoolNotStartedWithIngestDisabled() throws Exception {
        Settings settings = Settings.builder().put("name", "test").put("node.ingest", false).build();
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(settings);
            for (ThreadPool.Info info : threadPool.info()) {
                assertThat(info.getName(), not(equalTo("ingest")));
            }
        } finally {
            if (threadPool != null) {
                terminate(threadPool);
            }
        }
    }

    public void testIngestThreadPoolStartedWithIngestEnabled() throws Exception {
        Settings settings = Settings.builder().put("name", "test").put("node.ingest", true).build();
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(settings);
            boolean ingestFound = false;
            for (ThreadPool.Info info : threadPool.info()) {
                if (info.getName().equals("ingest")) {
                    ingestFound = true;
                    break;
                }
            }
            assertThat(ingestFound, equalTo(true));
        } finally {
            if (threadPool != null) {
                terminate(threadPool);
            }
        }
    }
}
