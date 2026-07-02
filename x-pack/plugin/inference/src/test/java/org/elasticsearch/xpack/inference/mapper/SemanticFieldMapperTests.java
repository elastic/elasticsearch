/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SemanticFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new InferencePlugin(Settings.EMPTY));
    }

    public void testSemanticFieldNotSupportedOnOldIndices() throws IOException {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());

        IndexVersion oldVersion = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.SEMANTIC_FIELD_TYPE);
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), oldVersion).build();

        var ex = expectThrows(MapperParsingException.class, () -> createMapperService(oldVersion, settings, mapping(b -> {
            b.startObject("my_field");
            b.field("type", SemanticFieldMapper.CONTENT_TYPE);
            b.field("inference_id", "test_model");
            b.endObject();
        })));
        assertThat(ex.getMessage(), containsString("[" + SemanticFieldMapper.CONTENT_TYPE + "]"));
        assertThat(ex.getMessage(), containsString("is not supported on indices created before version"));
        assertThat(ex.getMessage(), containsString(IndexVersions.SEMANTIC_FIELD_TYPE.toString()));
    }

    public void testSemanticFieldSupportedOnNewIndices() throws IOException {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());

        IndexVersion newVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE);
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), newVersion).build();

        // Should not throw; model_settings provided to avoid consulting the model registry
        var mapperService = createMapperService(newVersion, settings, mapping(b -> {
            b.startObject("my_field");
            b.field("type", SemanticFieldMapper.CONTENT_TYPE);
            b.field("inference_id", "test_model");
            b.startObject("model_settings");
            b.field("task_type", "embedding");
            b.field("dimensions", 128);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.endObject();
        }));
        assertNotNull(mapperService);
        assertSemanticFieldMapper(mapperService, "my_field");
    }

    public void testSemanticFieldMappingUpdateNotSupportedOnOldIndices() throws IOException {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());

        IndexVersion oldVersion = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.SEMANTIC_FIELD_TYPE);
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), oldVersion).build();

        var mapperService = createMapperService(oldVersion, settings, mapping(b -> {}));

        var ex = expectThrows(MapperParsingException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("my_field");
            b.field("type", SemanticFieldMapper.CONTENT_TYPE);
            b.field("inference_id", "test_model");
            b.endObject();
        })));
        assertThat(ex.getMessage(), containsString("[" + SemanticFieldMapper.CONTENT_TYPE + "]"));
        assertThat(ex.getMessage(), containsString("is not supported on indices created before version"));
        assertThat(ex.getMessage(), containsString(IndexVersions.SEMANTIC_FIELD_TYPE.toString()));
    }

    public void testSemanticFieldMappingUpdateSupportedOnNewIndices() throws IOException {
        assumeTrue("Semantic field feature flag is enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());

        IndexVersion newVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE);
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), newVersion).build();

        var mapperService = createMapperService(newVersion, settings, mapping(b -> {}));
        assertNotNull(mapperService);
        // Should not throw; model_settings provided to avoid consulting the model registry
        merge(mapperService, mapping(b -> {
            b.startObject("my_field");
            b.field("type", SemanticFieldMapper.CONTENT_TYPE);
            b.field("inference_id", "test_model");
            b.startObject("model_settings");
            b.field("task_type", "embedding");
            b.field("dimensions", 128);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.endObject();
        }));

        assertSemanticFieldMapper(mapperService, "my_field");
    }

    private static void assertSemanticFieldMapper(MapperService mapperService, String fieldName) {
        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertThat(mapper, instanceOf(SemanticFieldMapper.class));
    }
}
