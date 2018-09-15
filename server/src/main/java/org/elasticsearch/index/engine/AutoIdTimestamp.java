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

package org.elasticsearch.index.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks auto_id_timestamp of append-only index requests have been processed in an {@link Engine}.
 */
final class AutoIdTimestamp {
    private final AtomicLong maxUnsafeTimestamp = new AtomicLong(-1);
    private final AtomicLong maxSeenTimestamp = new AtomicLong(-1);

    void onNewTimestamp(long newTimestamp, boolean unsafe) {
        assert newTimestamp >= -1 : "invalid timestamp [" + newTimestamp + "]";
        maxSeenTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
        assert maxSeenTimestamp.get() >= newTimestamp;
        if (unsafe) {
            maxUnsafeTimestamp.updateAndGet(curr -> Math.max(curr, newTimestamp));
            assert maxUnsafeTimestamp.get() >= newTimestamp;
        }
        assert maxUnsafeTimestamp.get() <= maxSeenTimestamp.get();
    }

    long maxUnsafeTimestamp() {
        return maxUnsafeTimestamp.get();
    }

    long maxSeenTimestamp() {
        return maxSeenTimestamp.get();
    }
}
