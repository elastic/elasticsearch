/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Map;

public final class MlMetaIndex {

    private static final String INDEX_NAME = ".ml-meta";
    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";
    private static final int META_INDEX_MAPPINGS_VERSION = 1;

    /**
     * Where to store the ml info in Elasticsearch - must match what's
     * expected by kibana/engineAPI/app/directives/mlLogUsage.js
     *
     * @return The index name
     */
    public static String indexName() {
        return INDEX_NAME;
    }

    public static String mapping() {
        return TemplateUtils.loadTemplate(
            "/ml/meta_index_mappings.json",
            MlIndexAndAlias.BWC_MAPPINGS_VERSION, // Only needed for BWC with pre-8.10.0 nodes
            MAPPINGS_VERSION_VARIABLE,
            Map.of("xpack.ml.managed.index.version", Integer.toString(META_INDEX_MAPPINGS_VERSION))
        );
    }

    public static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private MlMetaIndex() {}
}
