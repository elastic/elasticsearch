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
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DestConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.dataframe.DataFrameField.TRANSFORM_ID;

public final class DataFrameInternalIndex {

    // constants for the index
    public static final String INDEX_TEMPLATE_VERSION = "1";
    public static final String INDEX_TEMPLATE_PATTERN = ".data-frame-internal-";
    public static final String INDEX_TEMPLATE_NAME = INDEX_TEMPLATE_PATTERN + INDEX_TEMPLATE_VERSION;
    public static final String INDEX_NAME = INDEX_TEMPLATE_NAME;

    public static final String AUDIT_TEMPLATE_VERSION = "1";
    public static final String AUDIT_INDEX_PREFIX = ".data-frame-notifications-";
    public static final String AUDIT_INDEX = AUDIT_INDEX_PREFIX + AUDIT_TEMPLATE_VERSION;

    // constants for mappings
    public static final String DYNAMIC = "dynamic";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";
    public static final String ENABLED = "enabled";
    public static final String DATE = "date";
    public static final String TEXT = "text";
    public static final String FIELDS = "fields";
    public static final String RAW = "raw";

    // data types
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String KEYWORD = "keyword";

    public static IndexTemplateMetaData getIndexTemplateMetaData() throws IOException {
        IndexTemplateMetaData dataFrameTemplate = IndexTemplateMetaData.builder(INDEX_TEMPLATE_NAME)
                .patterns(Collections.singletonList(INDEX_TEMPLATE_NAME))
                .version(Version.CURRENT.id)
                .settings(Settings.builder()
                        // the configurations are expected to be small
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
                .putMapping(MapperService.SINGLE_MAPPING_NAME, Strings.toString(mappings()))
                .build();
        return dataFrameTemplate;
    }

    public static IndexTemplateMetaData getAuditIndexTemplateMetaData() throws IOException {
        IndexTemplateMetaData dataFrameTemplate = IndexTemplateMetaData.builder(AUDIT_INDEX)
            .patterns(Collections.singletonList(AUDIT_INDEX_PREFIX + "*"))
            .version(Version.CURRENT.id)
            .settings(Settings.builder()
                // the audits are expected to be small
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
            .putMapping(MapperService.SINGLE_MAPPING_NAME, Strings.toString(auditMappings()))
            .build();
        return dataFrameTemplate;
    }

    private static XContentBuilder auditMappings() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder.startObject(SINGLE_MAPPING_NAME);
        addMetaInformation(builder);
        builder.field(DYNAMIC, "false");
        builder.startObject(PROPERTIES)
            .startObject(TRANSFORM_ID)
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(AbstractAuditMessage.LEVEL.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .startObject(AbstractAuditMessage.MESSAGE.getPreferredName())
            .field(TYPE, TEXT)
            .startObject(FIELDS)
            .startObject(RAW)
            .field(TYPE, KEYWORD)
            .endObject()
            .endObject()
            .endObject()
            .startObject(AbstractAuditMessage.TIMESTAMP.getPreferredName())
            .field(TYPE, DATE)
            .endObject()
            .startObject(AbstractAuditMessage.NODE_NAME.getPreferredName())
            .field(TYPE, KEYWORD)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        return builder;
    }

    private static XContentBuilder mappings() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();

        builder.startObject(MapperService.SINGLE_MAPPING_NAME);
        addMetaInformation(builder);

        // do not allow anything outside of the defined schema
        builder.field(DYNAMIC, "false");
        // the schema definitions
        builder.startObject(PROPERTIES);
        // overall doc type
        builder.startObject(DataFrameField.INDEX_DOC_TYPE.getPreferredName()).field(TYPE, KEYWORD).endObject();
        // add the schema for transform configurations
        addDataFrameTransformsConfigMappings(builder);
        // add the schema for transform stats
        addDataFrameTransformsStatsMappings(builder);
        // end type
        builder.endObject();
        // end properties
        builder.endObject();
        // end mapping
        builder.endObject();
        return builder;
    }


    private static XContentBuilder addDataFrameTransformsStatsMappings(XContentBuilder builder) throws IOException {
        return builder
            .startObject(DataFrameIndexerTransformStats.NUM_PAGES.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
            .startObject(DataFrameIndexerTransformStats.NUM_INPUT_DOCUMENTS.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.NUM_OUTPUT_DOCUMENTS.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.NUM_INVOCATIONS.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.INDEX_TIME_IN_MS.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.SEARCH_TIME_IN_MS.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.INDEX_TOTAL.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.SEARCH_TOTAL.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.SEARCH_FAILURES.getPreferredName())
                .field(TYPE, LONG)
            .endObject()
             .startObject(DataFrameIndexerTransformStats.INDEX_FAILURES.getPreferredName())
                .field(TYPE, LONG)
            .endObject();
    }

    private static XContentBuilder addDataFrameTransformsConfigMappings(XContentBuilder builder) throws IOException {
        return builder
            .startObject(DataFrameField.ID.getPreferredName())
                .field(TYPE, KEYWORD)
            .endObject()
            .startObject(DataFrameField.SOURCE.getPreferredName())
                .startObject(PROPERTIES)
                    .startObject(SourceConfig.INDEX.getPreferredName())
                        .field(TYPE, KEYWORD)
                    .endObject()
                    .startObject(SourceConfig.QUERY.getPreferredName())
                        .field(ENABLED, "false")
                    .endObject()
                .endObject()
            .endObject()
            .startObject(DataFrameField.DESTINATION.getPreferredName())
                .startObject(PROPERTIES)
                    .startObject(DestConfig.INDEX.getPreferredName())
                        .field(TYPE, KEYWORD)
                    .endObject()
                .endObject()
            .endObject()
            .startObject(DataFrameTransformConfig.DESCRIPTION.getPreferredName())
                .field(TYPE, TEXT)
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
