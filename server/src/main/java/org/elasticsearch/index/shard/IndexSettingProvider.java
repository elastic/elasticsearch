/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.Settings;

/**
 * An {@link IndexSettingProvider} is a provider for index level settings that can be set
 * explicitly as a default value (so they show up as "set" for newly created indices)
 */
public interface IndexSettingProvider {
    /**
     * Returns explicitly set default index {@link Settings} for the given index. This should not
     * return null.
     */
    default Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
        return Settings.EMPTY;
    }
}
