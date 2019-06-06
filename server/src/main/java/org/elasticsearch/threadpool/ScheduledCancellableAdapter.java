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

import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class ScheduledCancellableAdapter implements Scheduler.ScheduledCancellable {
    private final ScheduledFuture<?> scheduledFuture;

    ScheduledCancellableAdapter(ScheduledFuture<?> scheduledFuture) {
        assert scheduledFuture != null;
        this.scheduledFuture = scheduledFuture;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return scheduledFuture.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed other) {
        // unwrap other by calling on it.
        return -other.compareTo(scheduledFuture);
    }

    @Override
    public boolean cancel() {
        return FutureUtils.cancel(scheduledFuture);
    }

    @Override
    public boolean isCancelled() {
        return scheduledFuture.isCancelled();
    }
}
