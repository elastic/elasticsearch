/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.template.TemplateUtils;

/**
 * Class containing the index constants so that the index version, name, and prefix are available to a wider audience.
 */
public final class InferenceIndexConstants {

    /**
     * When incrementing the index version please also update
     * any use of the constant in x-pack/qa/
     *
     * version: 7.8.0:
     *  - adds inference_config definition to trained model config
     *
     * version: 7.10.0: 000003
     *  - adds trained_model_metadata object
     *
     * version: 8.0.0: 000004
     *  - adds binary_definition for TrainedModelDefinitionDoc
     */
    public static final String INDEX_VERSION = "000004";
    public static final String INDEX_NAME_PREFIX = ".ml-inference-";
    public static final String INDEX_PATTERN = INDEX_NAME_PREFIX + "*";
    public static final String LATEST_INDEX_NAME = INDEX_NAME_PREFIX + INDEX_VERSION;
    public static final ParseField DOC_TYPE = new ParseField("doc_type");

    private static final String MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";

    public static String mapping() {
        return TemplateUtils.loadTemplate(
            "/org/elasticsearch/xpack/core/ml/inference_index_mappings.json",
            Version.CURRENT.toString(),
            MAPPINGS_VERSION_VARIABLE);
    }

    public static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private InferenceIndexConstants() {}
}
