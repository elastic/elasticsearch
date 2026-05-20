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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
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

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "semantic");
        b.field("inference_id", "inference-id");
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return Map.of("type", "image", "value", "data:image/jpeg;base64,Y2F0IG9uIGEgd2luZG93c2lsbA==");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return null;
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
    protected void registerParameters(ParameterChecker checker) {
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

    public void testSourceInclusion() throws IOException {
        for (boolean includedInSource : new boolean[] { true, false }) {
            IndexVersion indexVersion = IndexVersion.current();
            var settings = Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
                .put(IndexSettings.INDEX_MAPPING_INCLUDE_SEMANTIC_FIELDS_IN_SOURCE_SETTING.getKey(), includedInSource)
                .build();
            final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
            MapperService mapperService = createMapperService(indexVersion, settings, fieldMapping);
            var fieldType = mapperService.fieldType("field");
            assertNotNull(fieldType);
            assertThat(fieldType, instanceOf(SemanticFieldMapper.SemanticFieldType.class));
            var semanticFieldType = (SemanticFieldMapper.SemanticFieldType) fieldType;
            assertEquals(includedInSource, semanticFieldType.isIncludedInSource());
        }
    }
}
