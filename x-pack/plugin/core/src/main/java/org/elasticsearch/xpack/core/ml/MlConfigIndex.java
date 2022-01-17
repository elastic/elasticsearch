/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.core.template.TemplateUtils;

public final class MlConfigIndex {

    private static final String INDEX_NAME = ".ml-config";
    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";

    public static final int CONFIG_INDEX_MAX_RESULTS_WINDOW = 10_000;

    /**
     * The name of the index where job, datafeed and analytics configuration is stored
     *
     * @return The index name
     */
    public static String indexName() {
        return INDEX_NAME;
    }

    public static String mapping() {
        return TemplateUtils.loadTemplate(
            "/org/elasticsearch/xpack/core/ml/config_index_mappings.json",
            Version.CURRENT.toString(),
            MAPPINGS_VERSION_VARIABLE
        );
    }

    public static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .build();
    }

    private MlConfigIndex() {}
}
