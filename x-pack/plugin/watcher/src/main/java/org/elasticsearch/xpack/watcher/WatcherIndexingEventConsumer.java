/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.xpack.core.watcher.watch.Watch;

interface WatcherIndexingEventConsumer {

    /// Atomically tries to schedule an active watch on the trigger engine and, only if the engine refused (it is
    /// paused between `pauseExecution` and `start`), retains the watch in the pending-watches map so the next reload
    /// picks it up. When the engine accepts the watch immediately, no pending entry is needed — the next reload will
    /// reload it from the index search anyway. Both branches happen under the same lock in [WatcherService], so a
    /// concurrent [#onWatchRemoved] cannot interleave between the engine call and the pending update and leave the
    /// two views inconsistent.
    void onWatchAdded(Watch watch);

    /// Atomically removes a watch from the pending-watches map and the trigger engine under the same lock as
    /// [#onWatchAdded]. This prevents a concurrent `postIndex` from resurrecting a deleted watch by sneaking an add
    /// in between the two halves of the removal.
    void onWatchRemoved(String watchId);
}
