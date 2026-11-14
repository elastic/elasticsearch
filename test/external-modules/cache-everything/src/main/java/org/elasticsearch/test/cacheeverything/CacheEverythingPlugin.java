/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cacheeverything;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * A test plugin that registers the test-only index setting {@code index.queries.cache.everything}, so that an index of a REST test
 * cluster can have every filter clause cached on first sight instead of waiting for the caching policy to admit it. Internal cluster
 * tests get the same setting from {@code InternalSettingsPlugin} in the test framework.
 */
public class CacheEverythingPlugin extends Plugin {

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING);
    }
}
