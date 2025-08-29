/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.Collection;
import java.util.Collections;

public class SemanticInferenceMetadataFieldsMapperTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new InferencePlugin(Settings.EMPTY));
    }

    public void testIsEnabled() {
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), getRandomCompatibleIndexVersion(true))
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true)
            .build();
        assertFalse(InferenceMetadataFieldsMapper.isEnabled(settings));

        settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), getRandomCompatibleIndexVersion(false))
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true)
            .build();
        assertFalse(InferenceMetadataFieldsMapper.isEnabled(settings));

        settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), getRandomCompatibleIndexVersion(false))
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), false)
            .build();
        assertTrue(InferenceMetadataFieldsMapper.isEnabled(settings));

        // Test that index.mapping.semantic_text.use_legacy_format == false is ignored when the index version is too old to support the new
        // format
        settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                IndexVersionUtils.randomVersionBetween(
                    random(),
                    IndexVersions.SEMANTIC_TEXT_FIELD_TYPE,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT)
                )  // 8.x version range prior to the introduction of the new format
            )
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), false)
            .build();
        assertFalse(InferenceMetadataFieldsMapper.isEnabled(settings));

        settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                IndexVersionUtils.randomVersionBetween(
                    random(),
                    IndexVersions.UPGRADE_TO_LUCENE_10_0_0,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.INFERENCE_METADATA_FIELDS)
                )  // 9.x version range prior to the introduction of the new format
            )
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), false)
            .build();
        assertFalse(InferenceMetadataFieldsMapper.isEnabled(settings));
    }

    public void testIsEnabledByDefault() {
        var settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                IndexVersionUtils.getPreviousVersion(InferenceMetadataFieldsMapper.USE_NEW_SEMANTIC_TEXT_FORMAT_BY_DEFAULT)
            )
            .build();
        assertFalse(InferenceMetadataFieldsMapper.isEnabled(settings));

        settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                InferenceMetadataFieldsMapper.USE_NEW_SEMANTIC_TEXT_FORMAT_BY_DEFAULT
            )
            .build();
        assertTrue(InferenceMetadataFieldsMapper.isEnabled(settings));

        // Test upgrades from 8.x
        settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT)
            )
            .build();
        assertFalse(InferenceMetadataFieldsMapper.isEnabled(settings));

        settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                IndexVersionUtils.randomVersionBetween(
                    random(),
                    IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                )
            )
            .build();
        assertTrue(InferenceMetadataFieldsMapper.isEnabled(settings));
    }

    @Override
    public void testFieldHasValue() {
        assertTrue(
            getMappedFieldType().fieldHasValue(
                new FieldInfos(new FieldInfo[] { getFieldInfoWithName(SemanticInferenceMetadataFieldsMapper.NAME) })
            )
        );
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assertFalse(getMappedFieldType().fieldHasValue(FieldInfos.EMPTY));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new SemanticInferenceMetadataFieldsMapper.FieldType();
    }

    static IndexVersion getRandomCompatibleIndexVersion(boolean useLegacyFormat) {
        if (useLegacyFormat) {
            // Randomly choose an index version compatible with the legacy semantic text format
            if (randomBoolean()) {
                // 9.x+ version
                return IndexVersionUtils.randomVersionBetween(random(), IndexVersions.UPGRADE_TO_LUCENE_10_0_0, IndexVersion.current());
            }

            // 8.x version
            return IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.SEMANTIC_TEXT_FIELD_TYPE,
                IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
            );
        } else {
            // Randomly choose an index version compatible with the new semantic text format
            if (randomBoolean()) {
                // 9.x+ version
                return IndexVersionUtils.randomVersionBetween(random(), IndexVersions.INFERENCE_METADATA_FIELDS, IndexVersion.current());
            }

            // 8.x version
            return IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT,
                IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
            );
        }
    }
}
