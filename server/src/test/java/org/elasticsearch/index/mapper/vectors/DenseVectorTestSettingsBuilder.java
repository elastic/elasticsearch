/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

class DenseVectorTestSettingsBuilder {
    static final Settings EXPERIMENTAL_FEATURES_ENABLED = new DenseVectorTestSettingsBuilder().experimentalFeatures(true).build();
    static final Settings EXPERIMENTAL_FEATURES_DISABLED = new DenseVectorTestSettingsBuilder().experimentalFeatures(false).build();

    private final Settings.Builder builder;

    DenseVectorTestSettingsBuilder() {
        this.builder = Settings.builder();
    }

    DenseVectorTestSettingsBuilder experimentalFeatures(boolean enabled) {
        builder.put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), enabled);
        return this;
    }

    DenseVectorTestSettingsBuilder indexDisabledByDefault(boolean disabled) {
        builder.put(IndexSettings.INDEX_DISABLED_BY_DEFAULT.getKey(), disabled);
        return this;
    }

    DenseVectorTestSettingsBuilder excludeSourceVectors(boolean exclude) {
        builder.put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), exclude);
        return this;
    }

    DenseVectorTestSettingsBuilder indexMode(IndexMode mode) {
        builder.put(IndexSettings.MODE.getKey(), mode.getName());
        return this;
    }

    DenseVectorTestSettingsBuilder intraMergeParallelism(boolean enabled) {
        builder.put(IndexSettings.INTRA_MERGE_PARALLELISM_ENABLED_SETTING.getKey(), enabled);
        return this;
    }

    Settings build() {
        return builder.build();
    }
}
