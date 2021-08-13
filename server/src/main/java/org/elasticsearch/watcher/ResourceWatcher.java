/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.watcher;

import java.io.IOException;

/**
 * Abstract resource watcher interface.
 * <p>
 * Different resource watchers can be registered with {@link ResourceWatcherService} to be called
 * periodically in order to check for changes in different external resources.
 */
public interface ResourceWatcher {
    /**
     * Called once when the resource watcher is added to {@link ResourceWatcherService}
     */
    void init() throws IOException;

    /**
     * Called periodically by {@link ResourceWatcherService} so resource watcher can check the resource
     */
    void checkAndNotify() throws IOException;
}
