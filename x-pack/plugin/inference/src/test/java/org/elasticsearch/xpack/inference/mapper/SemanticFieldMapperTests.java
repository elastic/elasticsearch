/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class SemanticFieldMapperTests extends MapperTestCase {
    private static String INFERENCE_ID = "inference-id";
    private ModelRegistry globalModelRegistry;
    private TestThreadPool threadPool;

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("Semantic field feature flag is not enabled", SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled());
    }

    @Before
    private void initializeTestEnvironment() {
        threadPool = createThreadPool();
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        var modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool));
        globalModelRegistry = spy(modelRegistry);
        globalModelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
        registerMultiModalEisEndpoint();
    }

    @After
    private void stopThreadPool() {
        threadPool.close();
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

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY) {
            @Override
            protected Supplier<ModelRegistry> getModelRegistry() {
                return () -> globalModelRegistry;
            }
        });
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
        b.field("inference_id", "inference-id");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return null;
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return
            Map.of("type", "image", "value", "data:image/jpeg;base64,Y2F0IG9uIGEgd2luZG93c2lsbA==");
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerIgnoredParameter("inference_id");
        checker.registerIgnoredParameter("search_inference_id");
        checker.registerIgnoredParameter("model_settings");
        checker.registerIgnoredParameter("index_options");
        checker.registerIgnoredParameter("chunking_settings");
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("doc_values are not supported in semantic", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(SemanticFieldMapper.SemanticFieldType.class));
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        // Until a doc is indexed, the query is rewritten as match no docs
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
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
}
