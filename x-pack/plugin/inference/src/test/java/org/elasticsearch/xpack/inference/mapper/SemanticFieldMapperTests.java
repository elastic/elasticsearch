/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.license.License;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class SemanticFieldMapperTests extends AbstractSemanticMapperTestCase {
    private static final String INFERENCE_ID = "inference-id";
    private static final String INFERENCE_ID = "inference-id";

    public SemanticFieldMapperTests(License.OperationMode operationMode) {
        super(operationMode);
    }

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("Semantic field feature flag is not enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected void registerDefaultEndpoints() {
        registerMultiModalEisEndpoint();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { License.OperationMode.BASIC }, new Object[] { License.OperationMode.ENTERPRISE });
    }

    private void registerMultiModalEisEndpoint() {
        globalModelRegistry.putDefaultIdIfAbsent(
            new InferenceService.DefaultConfigId(
                INFERENCE_ID,
                new MinimalServiceSettings(
                    ElasticInferenceService.NAME,
                    EMBEDDING,
                    1024,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                ),
                mock(InferenceService.class)
            )
        );
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

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "semantic");
        b.field("inference_id", INFERENCE_ID);
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return Map.of("type", "image", "value", "data:image/jpeg;base64,Y2F0IG9uIGEgd2luZG93c2lsbA==");
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(SemanticFieldMapper.SemanticFieldType.class));
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected Set<IndexVersion> getSupportedVersions() {
        return IndexVersionUtils.allReleasedVersions()
            .stream()
            .filter(v -> v.onOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE))
            .collect(Collectors.toSet());
    }

    @Override
    protected IndexVersion boostNotAllowedIndexVersion() {
        return IndexVersions.SEMANTIC_FIELD_TYPE;
    }

    @Override
    public final void testSupportsParsingObject() throws IOException {
        DocumentMapper mapper = createMapperService(fieldMapping(this::minimalMapping)).documentMapper();
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        Object sampleValueForDocument = getSampleObjectForDocument();
        assertThat(sampleValueForDocument, instanceOf(Map.class));
        SourceToParse source = source(builder -> {
            builder.field("field");
            builder.value(sampleValueForDocument);
        });
        ParsedDocument doc = mapper.parse(source);
        assertNotNull(doc);
    }

    public void testCustomInferenceIdIsMandatory() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> b.field("type", "semantic"))));

        assertThat(e.getMessage(), containsString("[inference_id] on mapper [field] of type [semantic] must not be empty"));
    }

    public void testInvalidInferenceEndpoints() {
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "semantic").field(INFERENCE_ID_FIELD, (String) null)))
            );
            assertThat(e.getMessage(), containsString("[inference_id] on mapper [field] of type [semantic] must not have a [null] value"));
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "semantic").field(INFERENCE_ID_FIELD, "")))
            );
            assertThat(e.getMessage(), containsString("[inference_id] on mapper [field] of type [semantic] must not be empty"));
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    fieldMapping(
                        b -> b.field("type", "semantic").field(INFERENCE_ID_FIELD, INFERENCE_ID).field(SEARCH_INFERENCE_ID_FIELD, "")
                    )
                )
            );
            assertThat(e.getMessage(), containsString("[search_inference_id] on mapper [field] of type [semantic] must not be empty"));
        }
    }
}
