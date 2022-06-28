/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.client;

import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;

public final class WatchSourceBuilders {

    private WatchSourceBuilders() {}

    public static WatchSourceBuilder watchBuilder() {
        return new WatchSourceBuilder();
    }
}
