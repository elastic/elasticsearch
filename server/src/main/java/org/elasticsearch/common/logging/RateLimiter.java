/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * This class limits the number of times that actions are carried out.
 * <p>
 * The throttling algorithm relies on a LRU set of keys, which evicts entries when its size exceeds 128.
 * When a {@code key} is seen for the first time, the {@code runnable} will be executed, but then will not be
 * executed again for that key until the key is removed from the set.
 */
public class RateLimiter {

    // LRU set of keys used to determine if a message should be emitted to the logs
    private final Set<String> keys = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
            return size() > 128;
        }
    }));

    public void limit(String key, Runnable runnable) {
        boolean shouldRun = keys.add(key);
        if (shouldRun) {
            runnable.run();
        }
    }
}
