/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants.LATEST_INDEX_NAME;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DATE;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DYNAMIC;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.ENABLED;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.KEYWORD;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.LONG;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.PROPERTIES;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.TEXT;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.TYPE;
import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.addMetaInformation;

/**
 * Changelog of internal index versions
 *
 * Please list changes, increase the version in {@link InferenceInternalIndex} if you are 1st in this release cycle
 *
 * version 1 (7.5): initial
 */
public final class InferenceInternalIndex {

    private InferenceInternalIndex() {}

    public static XContentBuilder mappings() throws IOException {
        return configMapping(SINGLE_MAPPING_NAME);
    }

    public static IndexTemplateMetaData getIndexTemplateMetaData() throws IOException {
        IndexTemplateMetaData inferenceTemplate = IndexTemplateMetaData.builder(LATEST_INDEX_NAME)
            .patterns(Collections.singletonList(LATEST_INDEX_NAME))
            .version(Version.CURRENT.id)
            .settings(Settings.builder()
                // the configurations are expected to be small
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
            .putMapping(SINGLE_MAPPING_NAME, Strings.toString(mappings()))
            .build();
        return inferenceTemplate;
    }

    public static XContentBuilder configMapping(String mappingType) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        builder.startObject(mappingType);
        addMetaInformation(builder);

        // do not allow anything outside of the defined schema
        builder.field(DYNAMIC, "false");

        builder.startObject(PROPERTIES);

        // Add the doc_type field
        builder.startObject(InferenceIndexConstants.DOC_TYPE.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject();

        addInferenceDocFields(builder);
        addDefinitionDocFields(builder);
        return builder.endObject()
            .endObject()
            .endObject();
    }

    private static void addInferenceDocFields(XContentBuilder builder) throws IOException {
        builder.startObject(TrainedModelConfig.MODEL_ID.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TrainedModelConfig.CREATED_BY.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TrainedModelConfig.INPUT.getPreferredName())
            .field(ENABLED, false)
            .endObject()
            .startObject(TrainedModelConfig.VERSION.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TrainedModelConfig.DESCRIPTION.getPreferredName())
            .field(TYPE, TEXT)
            .endObject()
            .startObject(TrainedModelConfig.CREATE_TIME.getPreferredName())
            .field(TYPE, DATE)
            .endObject()
            .startObject(TrainedModelConfig.TAGS.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(TrainedModelConfig.METADATA.getPreferredName())
            .field(ENABLED, false)
            .endObject()
            .startObject(TrainedModelConfig.ESTIMATED_OPERATIONS.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TrainedModelConfig.ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName())
            .field(TYPE, LONG)
            .endObject();
    }

    private static void addDefinitionDocFields(XContentBuilder builder) throws IOException {
        builder.startObject(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TrainedModelDefinitionDoc.DEFINITION.getPreferredName())
            .field(ENABLED, false)
            .endObject()
            .startObject(TrainedModelDefinitionDoc.COMPRESSION_VERSION.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TrainedModelDefinitionDoc.DEFINITION_LENGTH.getPreferredName())
            .field(TYPE, LONG)
            .endObject()
            .startObject(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName())
            .field(TYPE, LONG)
            .endObject();
    }
}
