/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * This plugin is used to register the {@link SearchService#MINIMUM_DOCS_PER_SLICE} setting.
 * This setting forces the {@link SearchService} to create many slices even when very few documents
 * are available, something we don;t really want to happen in real usage.
 */
public class ConcurrentSearchTestPlugin extends Plugin {
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SearchService.MINIMUM_DOCS_PER_SLICE);
    }
}
