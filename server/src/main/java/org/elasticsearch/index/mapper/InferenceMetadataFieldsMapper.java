/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Map;
import java.util.function.Function;

/**
 * An abstract {@link MetadataFieldMapper} used as a placeholder for implementation
 * in the inference module. It is required by {@link SourceFieldMapper} to identify
 * the field name for removal from _source.
 */
public abstract class InferenceMetadataFieldsMapper extends MetadataFieldMapper {
    /**
     * Internal index setting to control the format used for semantic text fields.
     * Determines whether to use the legacy format (default: true).
     * This setting is immutable and can only be defined at index creation
     * to ensure the internal format remains consistent throughout the index's lifecycle.
     */
    public static final Setting<Boolean> USE_LEGACY_SEMANTIC_TEXT_FORMAT = Setting.boolSetting(
        "index.mapping.semantic_text.use_legacy_format",
        false,
        Setting.Property.Final,
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    // Check index version SOURCE_MAPPER_MODE_ATTRIBUTE_NOOP because that index version was added in the same serverless promotion
    // where the new format was enabled by default
    public static final IndexVersion USE_NEW_SEMANTIC_TEXT_FORMAT_BY_DEFAULT = IndexVersions.SOURCE_MAPPER_MODE_ATTRIBUTE_NOOP;

    public static final String NAME = "_inference_fields";
    public static final String CONTENT_TYPE = "_inference_fields";

    protected InferenceMetadataFieldsMapper(MappedFieldType inferenceFieldType) {
        super(inferenceFieldType);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public InferenceMetadataFieldType fieldType() {
        return (InferenceMetadataFieldType) super.fieldType();
    }

    public abstract static class InferenceMetadataFieldType extends MappedFieldType {
        public InferenceMetadataFieldType() {
            super(NAME, false, false, false, TextSearchInfo.NONE, Map.of());
        }

        /**
         * Returns a {@link ValueFetcher} without requiring the construction of a full {@link SearchExecutionContext}.
         */
        public abstract ValueFetcher valueFetcher(
            MappingLookup mappingLookup,
            Function<Query, BitSetProducer> bitSetCache,
            IndexSearcher searcher
        );
    }

    /**
     * Checks if the {@link InferenceMetadataFieldsMapper} is enabled for the given {@link Settings}.
     *
     * This indicates whether the new format for semantic text fields is active.
     * The new format is enabled if:
     * 1. The index version is on or after {@link IndexVersions#INFERENCE_METADATA_FIELDS}, and
     * 2. The legacy semantic text format is disabled.
     *
     * @param settings the index settings to evaluate
     * @return {@code true} if the new format is enabled; {@code false} otherwise
     */
    public static boolean isEnabled(Settings settings) {
        var version = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
        if ((version.before(IndexVersions.INFERENCE_METADATA_FIELDS)
            && version.between(IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT, IndexVersions.UPGRADE_TO_LUCENE_10_0_0) == false)
            || (version.onOrAfter(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                && version.before(USE_NEW_SEMANTIC_TEXT_FORMAT_BY_DEFAULT)
                && USE_LEGACY_SEMANTIC_TEXT_FORMAT.exists(settings) == false)) {
            return false;
        }

        return USE_LEGACY_SEMANTIC_TEXT_FORMAT.get(settings) == false;
    }

    /**
     * Checks if the {@link InferenceMetadataFieldsMapper} is enabled based on the provided {@link Mapping}.
     *
     * This indicates whether the new format for semantic text fields is active by verifying the existence
     * of the {@link InferenceMetadataFieldsMapper} in the mapping's metadata.
     *
     * @param mappingLookup the mapping to evaluate
     * @return {@code true} if the {@link InferenceMetadataFieldsMapper} is present; {@code false} otherwise
     */
    public static boolean isEnabled(MappingLookup mappingLookup) {
        return mappingLookup != null
            && mappingLookup.getMapping()
                .getMetadataMapperByName(InferenceMetadataFieldsMapper.NAME) instanceof InferenceMetadataFieldsMapper;
    }
}
