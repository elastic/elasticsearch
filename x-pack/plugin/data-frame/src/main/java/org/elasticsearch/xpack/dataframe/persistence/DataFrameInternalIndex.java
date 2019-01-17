/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public final class DataFrameInternalIndex {

    // constants for the index
    public static final String INDEX_TEMPLATE_VERSION = "1";
    public static final String INDEX_TEMPLATE_PATTERN = ".data-frame-internal-";
    public static final String INDEX_TEMPLATE_NAME = INDEX_TEMPLATE_PATTERN + INDEX_TEMPLATE_VERSION;
    public static final String INDEX_NAME = INDEX_TEMPLATE_NAME;

    // constants for mappings
    public static final String ENABLED = "enabled";
    public static final String DYNAMIC = "dynamic";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";

    // data types
    public static final String DOUBLE = "double";
    public static final String KEYWORD = "keyword";

    // internal document types, e.g. "transform_config"
    public static final String DOC_TYPE = "doc_type";

    public static IndexTemplateMetaData getIndexTemplateMetaData() throws IOException {
        IndexTemplateMetaData dataFrameTemplate = IndexTemplateMetaData.builder(INDEX_TEMPLATE_NAME)
                .patterns(Collections.singletonList(INDEX_TEMPLATE_NAME))
                .version(Version.CURRENT.id)
                .settings(Settings.builder()
                        // the configurations are expected to be small
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
                // todo: remove type
                .putMapping(MapperService.SINGLE_MAPPING_NAME, Strings.toString(mappings()))
                .build();
        return dataFrameTemplate;
    }

    private static XContentBuilder mappings() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();

        builder.startObject(MapperService.SINGLE_MAPPING_NAME);
        addMetaInformation(builder);

        // no need to analyze anything, we use the config index as key value store, revisit if we decide to search on it
        builder.field(ENABLED, false);
        // do not allow anything outside of the defined schema
        builder.field(DYNAMIC, "strict");
        // the schema definitions
        builder.startObject(PROPERTIES);
        // overall doc type
        builder.startObject(DOC_TYPE).field(TYPE, KEYWORD).endObject();
        // add the schema for transform configurations
        addDataFrameTransformsConfigMappings(builder);

        // end type
        builder.endObject();
        // end properties
        builder.endObject();
        // end mapping
        builder.endObject();
        return builder;
    }

    private static XContentBuilder addDataFrameTransformsConfigMappings(XContentBuilder builder) throws IOException {
        return builder
            .startObject(DataFrameField.ID.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject();
    }

    /**
     * Inserts "_meta" containing useful information like the version into the mapping
     * template.
     *
     * @param builder The builder for the mappings
     * @throws IOException On write error
     */
    private static XContentBuilder addMetaInformation(XContentBuilder builder) throws IOException {
        return builder.startObject("_meta")
                    .field("version", Version.CURRENT)
                .endObject();
    }

    private DataFrameInternalIndex() {
    }
}
