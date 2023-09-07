/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.utils;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

/**
 * A utility to wait for a version to be available, assuming a monotonically increasing versioning scheme like cluster state.
 */
public class WaitForVersion {
    private final Map<LongPredicate, Runnable> waiters = ConcurrentCollections.newConcurrentMap();
    private final AtomicLong lastProcessedVersion = new AtomicLong(-1);

    public void waitUntilVersion(long version, Runnable action) {
        if (version <= lastProcessedVersion.get()) {
            action.run();
        } else {
            waiters.put(value -> value <= lastProcessedVersion.get(), action);
            long versionProcessedAfter = lastProcessedVersion.get();
            if (version <= versionProcessedAfter) {
                retest(versionProcessedAfter);
            }
        }
    }

    public void notifyVersionProcessed(long versionProcessed) {
        if (versionProcessed > lastProcessedVersion.get()) {
            long result = lastProcessedVersion.accumulateAndGet(versionProcessed, Math::max);
            if (result == versionProcessed) {
                retest(versionProcessed);
            }
        }
    }

    private void retest(long versionProcessed) {
        waiters.keySet().forEach(key -> {
            if (key.test(versionProcessed)) {
                Runnable action = waiters.remove(key);
                if (action != null) {
                    action.run();
                }
            }
        });
    }
}
