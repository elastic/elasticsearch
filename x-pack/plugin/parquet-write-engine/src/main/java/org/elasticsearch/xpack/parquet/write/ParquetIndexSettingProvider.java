/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Adds the settings that an index with {@code index.storage_format: parquet} needs and that the user should not have to repeat:
 * <ul>
 *   <li>no replicas unless the request asks for them (a replica is a search replica, see {@link ParquetShardRoutingRoleStrategy});</li>
 *   <li>batch indexing, the only write path of the engine;</li>
 *   <li>single-valued doc values, until the request asks for lists;</li>
 *   <li>an {@code _id} without postings, which the search replicas synthesize from the row number;</li>
 *   <li>a refresh interval of {@link ParquetFormatSettings#DEFAULT_REFRESH_INTERVAL}, because each scheduled refresh of a search replica
 *       reads the table, and the primary tells its replicas of each upload anyway;</li>
 *   <li>and synthetic source.</li>
 * </ul>
 * Settings that the user or a template sets explicitly win, and then the validator of {@link ParquetFormatSettings#INDEX_FORMAT}
 * rejects the ones that the format cannot accept.
 */
final class ParquetIndexSettingProvider implements IndexSettingProvider {

    @Override
    public void provideAdditionalSettings(
        String indexName,
        @Nullable String dataStreamName,
        @Nullable IndexMode templateIndexMode,
        boolean registryInstalledTemplate,
        ProjectMetadata projectMetadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings,
        IndexVersion indexVersion,
        Settings.Builder additionalSettings
    ) {
        if (ParquetFormatSettings.INDEX_FORMAT.get(indexTemplateAndCreateRequestSettings) != ParquetFormatSettings.IndexFormat.PARQUET) {
            return;
        }
        validate(templateIndexMode, indexTemplateAndCreateRequestSettings);
        final String multiValueKey = FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey();
        ParquetMappingValidator.validate(
            "[" + ParquetFormatSettings.INDEX_FORMAT.getKey() + "] [parquet]",
            combinedTemplateMappings,
            indexTemplateAndCreateRequestSettings.hasValue(multiValueKey)
                && Booleans.parseBoolean(indexTemplateAndCreateRequestSettings.get(multiValueKey))
        );
        // a replica is a search replica, which reads the uploaded files, so it is the user's choice; the default is none
        if (indexTemplateAndCreateRequestSettings.hasValue(IndexMetadata.SETTING_NUMBER_OF_REPLICAS) == false) {
            additionalSettings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        }
        if (indexTemplateAndCreateRequestSettings.hasValue(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()) == false) {
            additionalSettings.put(
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),
                ParquetFormatSettings.DEFAULT_REFRESH_INTERVAL.getStringRep()
            );
        }
        requireSetting(indexTemplateAndCreateRequestSettings, additionalSettings, IndexSettings.BATCH_INDEXING.getKey(), true);
        // the files have no _id: a search replica synthesizes it from the row number, so a query on it scans the doc values
        requireSetting(indexTemplateAndCreateRequestSettings, additionalSettings, IndexSettings.ID_FIELD_INDEXED.getKey(), false);
        // Fields are flat columns unless the user asks for lists: a field with "multi_value": true, or the index default set to true.
        if (indexTemplateAndCreateRequestSettings.hasValue(multiValueKey) == false) {
            additionalSettings.put(multiValueKey, false);
        }
        // The columnar index mode already makes the source synthetic, and on a node with the logsdb plugin another provider sets this key,
        // and the server refuses two providers that set the same key. So we do not set it, and we only refuse another explicit value.
        final String sourceModeKey = IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey();
        if (indexTemplateAndCreateRequestSettings.hasValue(sourceModeKey)
            && SourceFieldMapper.Mode.SYNTHETIC.toString()
                .equalsIgnoreCase(indexTemplateAndCreateRequestSettings.get(sourceModeKey)) == false) {
            throw new IllegalArgumentException(
                "["
                    + ParquetFormatSettings.INDEX_FORMAT.getKey()
                    + "] [parquet] requires ["
                    + sourceModeKey
                    + "] [synthetic] but was ["
                    + indexTemplateAndCreateRequestSettings.get(sourceModeKey)
                    + "]"
            );
        }
    }

    /**
     * Sets a value that the format needs. The server refuses a provider that sets a key that the request already has, even with the same
     * value, so a key that the user (or a template) set to the needed value is left alone, and one with another value is an error with a
     * message that names the format.
     */
    private static void requireSetting(Settings requestSettings, Settings.Builder additionalSettings, String key, Object required) {
        if (requestSettings.hasValue(key)) {
            final String actual = requestSettings.get(key);
            if (actual.equalsIgnoreCase(required.toString()) == false) {
                throw new IllegalArgumentException(
                    "["
                        + ParquetFormatSettings.INDEX_FORMAT.getKey()
                        + "] [parquet] requires ["
                        + key
                        + "] ["
                        + required
                        + "] but was ["
                        + actual
                        + "]"
                );
            }
            return;
        }
        additionalSettings.put(key, required.toString());
    }

    /** The rules of the format, checked on the settings of the request and the templates together. */
    private static void validate(@Nullable IndexMode templateIndexMode, Settings settings) {
        final String format = "[" + ParquetFormatSettings.INDEX_FORMAT.getKey() + "] [parquet]";
        final IndexMode mode = templateIndexMode != null ? templateIndexMode : IndexSettings.MODE.get(settings);
        if (mode != IndexMode.COLUMNAR) {
            throw new IllegalArgumentException(format + " requires [index.mode] [columnar] but was [" + mode + "]");
        }
        if (IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).enabled()) {
            throw new IllegalArgumentException(
                format + " does not support [index.auto_expand_replicas], set [index.number_of_replicas] to the search replicas it needs"
            );
        }
        if (ParquetFormatSettings.REPOSITORY.get(settings).isEmpty()) {
            throw new IllegalArgumentException(format + " requires [" + ParquetFormatSettings.REPOSITORY.getKey() + "]");
        }
    }

    @Override
    public Set<String> affectedSettings() {
        return Set.of(
            IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),
            IndexSettings.BATCH_INDEXING.getKey(),
            IndexSettings.ID_FIELD_INDEXED.getKey(),
            FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey()
        );
    }
}
