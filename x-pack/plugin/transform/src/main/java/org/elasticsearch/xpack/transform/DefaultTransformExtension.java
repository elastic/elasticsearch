/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

public class DefaultTransformExtension implements TransformExtension {

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);

    @Override
    public boolean includeNodeInfo() {
        return true;
    }

    @Override
    public Settings getTransformInternalIndexAdditionalSettings() {
        return Settings.EMPTY;
    }

    /**
     * Provides destination index settings, hardcoded at the moment. In future this might be customizable or generation could be based on
     * source settings.
     */
    @Override
    public Settings getTransformDestinationIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    @Override
    public TimeValue getMinFrequency() {
        return MIN_FREQUENCY;
    }
}
