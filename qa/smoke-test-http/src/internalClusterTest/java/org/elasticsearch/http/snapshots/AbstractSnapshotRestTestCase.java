/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.snapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.http.HttpSmokeTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.util.Collection;

public abstract class AbstractSnapshotRestTestCase extends HttpSmokeTestCase {

    /**
     * We use single threaded metadata fetching in some tests to make sure that once the snapshot meta thread is stuck on a blocked repo,
     * no other snapshot meta thread can concurrently finish a request/task
     */
    protected static final Settings SINGLE_THREADED_SNAPSHOT_META_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot_meta.core", 1)
        .put("thread_pool.snapshot_meta.max", 1)
        .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }
}
