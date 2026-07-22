/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher;

import org.elasticsearch.xpack.core.watcher.watch.Watch;

/// This interface is used to notify the [WatcherService] about changes in the watch store.
interface WatcherEventConsumer {

    void onWatchAdded(Watch watch);

    void onWatchRemoved(String watchId);
}
