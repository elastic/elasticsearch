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

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class ESThreadPoolTestCase extends ESTestCase {

    protected final ThreadPool.Info info(final ThreadPool threadPool, final String name) {
        for (final ThreadPool.Info info : threadPool.info()) {
            if (info.getName().equals(name)) {
                return info;
            }
        }
        assert "same".equals(name);
        return null;
    }

    protected final ThreadPoolStats.Stats stats(final ThreadPool threadPool, final String name) {
        for (final ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (name.equals(stats.getName())) {
                return stats;
            }
        }
        throw new IllegalArgumentException(name);
    }

    protected final void terminateThreadPoolIfNeeded(final ThreadPool threadPool) throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    static String randomThreadPool(final ThreadPool.ThreadPoolType type) {
        return randomFrom(
                ThreadPool.THREAD_POOL_TYPES
                        .entrySet().stream()
                        .filter(t -> t.getValue().equals(type))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList()));
    }

}
