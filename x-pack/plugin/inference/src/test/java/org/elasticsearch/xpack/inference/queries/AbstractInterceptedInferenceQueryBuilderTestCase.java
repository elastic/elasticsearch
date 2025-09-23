/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.TransportVersions.NEW_SEMANTIC_QUERY_INTERCEPTORS;
import static org.elasticsearch.TransportVersions.SEMANTIC_SEARCH_CCS_SUPPORT;
import static org.elasticsearch.TransportVersions.V_8_15_0;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public abstract class AbstractInterceptedInferenceQueryBuilderTestCase<T extends AbstractQueryBuilder<T>> extends MapperServiceTestCase {
    private static TestThreadPool threadPool;
    private static ModelRegistry modelRegistry;

    protected static final String SPARSE_INFERENCE_ID = "sparse-inference-id";
    protected static final MinimalServiceSettings SPARSE_INFERENCE_ID_SETTINGS = new MinimalServiceSettings(
        null,
        TaskType.SPARSE_EMBEDDING,
        null,
        null,
        null
    );

    protected static final String DENSE_INFERENCE_ID = "dense-inference-id";
    protected static final MinimalServiceSettings DENSE_INFERENCE_ID_SETTINGS = new MinimalServiceSettings(
        null,
        TaskType.TEXT_EMBEDDING,
        256,
        SimilarityMeasure.COSINE,
        DenseVectorFieldMapper.ElementType.FLOAT
    );

    private static final Map<String, MinimalServiceSettings> INFERENCE_ENDPOINT_MAP = Map.of(
        SPARSE_INFERENCE_ID,
        SPARSE_INFERENCE_ID_SETTINGS,
        DENSE_INFERENCE_ID,
        DENSE_INFERENCE_ID_SETTINGS
    );

    private NamedWriteableRegistry namedWriteableRegistry = null;

    private static class InferencePluginWithModelRegistry extends InferencePlugin {
        InferencePluginWithModelRegistry(Settings settings) {
            super(settings);
        }

        @Override
        protected Supplier<ModelRegistry> getModelRegistry() {
            return () -> modelRegistry;
        }
    }

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(AbstractInterceptedInferenceQueryBuilderTestCase.class.getName());
        modelRegistry = createModelRegistry(threadPool);
    }

    @AfterClass
    public static void afterClass() {
        threadPool.close();
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePluginWithModelRegistry(Settings.EMPTY));
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        if (namedWriteableRegistry == null) {
            namedWriteableRegistry = new NamedWriteableRegistry(getNamedWriteables());
        }

        return namedWriteableRegistry;
    }

    @Override
    public void testFieldHasValue() {
        assumeTrue("random test inherited from MapperServiceTestCase", false);
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assumeTrue("random test inherited from MapperServiceTestCase", false);
    }

    public void testSerialization() throws Exception {
        serializationTestCase(TransportVersion.current());
    }

    public void testBwCSerialization() throws Exception {
        TransportVersion minTransportVersion = TransportVersion.max(getMinimalSupportedVersion(), TransportVersion.minimumCompatible());
        for (int i = 0; i < 100; i++) {
            TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                minTransportVersion,
                TransportVersionUtils.getPreviousVersion(TransportVersion.current())
            );
            serializationTestCase(transportVersion);
        }
    }

    public void testCcsSerialization() throws Exception {
        final String inferenceField = "semantic_field";
        final var localIndexInferenceFields = Map.of("local-index", Map.of(inferenceField, SPARSE_INFERENCE_ID));
        final var remoteIndices = Map.of("remote-alias", "remote-index");
        final T inferenceFieldQuery = createQueryBuilder(inferenceField);
        final T nonInferenceFieldQuery = createQueryBuilder("non_inference_field");

        // Test with the current transport version. This simulates sending the query to a remote cluster that supports semantic search CCS.
        final QueryRewriteContext contextCurrent = createQueryRewriteContext(
            localIndexInferenceFields,
            remoteIndices,
            TransportVersion.current(),
            true
        );

        assertRewriteAndSerializeOnInferenceField(inferenceFieldQuery, contextCurrent, null, null);
        assertRewriteAndSerializeOnNonInferenceField(nonInferenceFieldQuery, contextCurrent);

        // Test when ccs_minimize_roundtrips=false
        final QueryRewriteContext minimizeRoundTripsFalseContext = createQueryRewriteContext(
            localIndexInferenceFields,
            remoteIndices,
            TransportVersion.current(),
            false
        );

        assertRewriteAndSerializeOnInferenceField(
            inferenceFieldQuery,
            minimizeRoundTripsFalseContext,
            new IllegalArgumentException(
                inferenceFieldQuery.getName()
                    + " query does not support cross-cluster search when querying a ["
                    + SemanticTextFieldMapper.CONTENT_TYPE
                    + "] field when [ccs_minimize_roundtrips] is false"
            ),
            null
        );
        assertRewriteAndSerializeOnNonInferenceField(nonInferenceFieldQuery, minimizeRoundTripsFalseContext);

        // Test with a transport version prior to semantic search CCS support, but still new enough to use the new interceptors.
        // This simulates if one of the local or remote cluster data nodes is slightly out of date.
        final TransportVersion preCcsVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            NEW_SEMANTIC_QUERY_INTERCEPTORS,
            TransportVersionUtils.getPreviousVersion(SEMANTIC_SEARCH_CCS_SUPPORT)
        );
        final QueryRewriteContext preCcsContext = createQueryRewriteContext(localIndexInferenceFields, remoteIndices, preCcsVersion, true);

        assertRewriteAndSerializeOnInferenceField(
            inferenceFieldQuery,
            preCcsContext,
            null,
            new IllegalArgumentException(
                "One or more nodes does not support "
                    + inferenceFieldQuery.getName()
                    + " query cross-cluster search when querying a ["
                    + SemanticTextFieldMapper.CONTENT_TYPE
                    + "] field. Please update all nodes to at least Elasticsearch "
                    + SEMANTIC_SEARCH_CCS_SUPPORT.toReleaseVersion()
                    + "."
            )
        );
        assertRewriteAndSerializeOnNonInferenceField(nonInferenceFieldQuery, preCcsContext);

        // Test with a transport version prior to the new query interceptors. This simulates if one of the local cluster data nodes is more
        // out of date.
        final TransportVersion legacyInterceptorsVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            V_8_15_0,
            TransportVersionUtils.getPreviousVersion(NEW_SEMANTIC_QUERY_INTERCEPTORS)
        );
        final QueryRewriteContext legacyInterceptorsContext = createQueryRewriteContext(
            localIndexInferenceFields,
            remoteIndices,
            legacyInterceptorsVersion,
            true
        );

        assertRewriteAndSerializeOnInferenceField(
            inferenceFieldQuery,
            legacyInterceptorsContext,
            new IllegalArgumentException(
                inferenceFieldQuery.getName()
                    + " query does not support cross-cluster search when querying a ["
                    + SemanticTextFieldMapper.CONTENT_TYPE
                    + "] field in a mixed-version cluster"
            ),
            null
        );
        assertRewriteAndSerializeOnNonInferenceField(nonInferenceFieldQuery, legacyInterceptorsContext);
    }

    public void testSerializationRemoteClusterInferenceResults() throws Exception {
        InferenceResults inferenceResults1 = new TextExpansionResults(
            DEFAULT_RESULTS_FIELD,
            List.of(new WeightedToken("foo", 1.0f)),
            false
        );
        InferenceResults inferenceResults2 = new TextExpansionResults(
            DEFAULT_RESULTS_FIELD,
            List.of(new WeightedToken("bar", 2.0f)),
            false
        );

        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap = Map.of(
            new FullyQualifiedInferenceId(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            inferenceResults1,
            new FullyQualifiedInferenceId(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            inferenceResults2
        );

        // It doesn't matter that the original query doesn't refer to an inference ID in the inference results map or if the inference
        // results in the map don't match the type expected by the query. This only tests serialization, so it only matters that both
        // the original query and the inference results map exists.
        QueryBuilder interceptedQuery = createInterceptedQueryBuilder(createQueryBuilder(randomAlphaOfLength(5)), inferenceResultsMap);

        // Test with the current transport version, which should work
        QueryBuilder deserializedQuery = copyNamedWriteable(interceptedQuery, writableRegistry(), QueryBuilder.class);
        assertThat(deserializedQuery, equalTo(interceptedQuery));

        // Test with a transport version prior to cluster alias support, which should fail
        TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            NEW_SEMANTIC_QUERY_INTERCEPTORS,
            TransportVersionUtils.getPreviousVersion(TransportVersions.INFERENCE_RESULTS_MAP_WITH_CLUSTER_ALIAS)
        );
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> copyNamedWriteable(interceptedQuery, writableRegistry(), QueryBuilder.class, transportVersion)
        );
        assertThat(e.getMessage(), equalTo("Cannot serialize remote cluster inference results in a mixed-version cluster"));
    }

    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        getPlugins().forEach(plugin -> entries.addAll(plugin.getNamedWriteables()));
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());

        SearchModule searchModule = new SearchModule(
            Settings.EMPTY,
            getPlugins().stream().filter(p -> p instanceof SearchPlugin).map(p -> (SearchPlugin) p).toList()
        );
        entries.addAll(searchModule.getNamedWriteables());

        return entries;
    }

    protected abstract T createQueryBuilder(String field);

    protected abstract InterceptedInferenceQueryBuilder<T> createInterceptedQueryBuilder(
        T originalQuery,
        Map<FullyQualifiedInferenceId, InferenceResults> inferenceResultsMap
    );

    protected abstract QueryRewriteInterceptor createQueryRewriteInterceptor();

    protected abstract TransportVersion getMinimalSupportedVersion();

    protected abstract void assertCoordinatorNodeRewriteOnInferenceField(
        QueryBuilder original,
        QueryBuilder rewritten,
        TransportVersion transportVersion,
        QueryRewriteContext queryRewriteContext
    );

    protected abstract void assertCoordinatorNodeRewriteOnNonInferenceField(QueryBuilder original, QueryBuilder rewritten);

    protected void serializationTestCase(TransportVersion transportVersion) throws Exception {
        final String semanticField = "semantic_field";
        final String mixedField = "mixed_field";
        final String textField = "text_field";
        final TestIndex testIndex1 = new TestIndex(
            "test-index-1",
            Map.of(semanticField, SPARSE_INFERENCE_ID, mixedField, DENSE_INFERENCE_ID),
            Map.of(textField, Map.of("type", "text"))
        );
        final TestIndex testIndex2 = new TestIndex(
            "test-index-2",
            Map.of(semanticField, SPARSE_INFERENCE_ID),
            Map.of(mixedField, Map.of("type", "text"), textField, Map.of("type", "text"))
        );
        final QueryRewriteContext queryRewriteContext = createQueryRewriteContext(
            Map.of(testIndex1.name(), testIndex1.semanticTextFields(), testIndex2.name(), testIndex2.semanticTextFields()),
            Map.of(),
            transportVersion,
            null
        );

        // Disable query interception when checking the results of coordinator node rewrite so that the query rewrite context can be used
        // to populate inference results without triggering another query interception. In production this is achieved by wrapping with
        // InterceptedQueryBuilderWrapper, but we do not have access to that in this test.

        // Query a semantic text field in both indices
        QueryBuilder originalSemantic = createQueryBuilder(semanticField);
        QueryBuilder rewrittenSemantic = rewriteAndFetch(originalSemantic, queryRewriteContext);
        QueryBuilder serializedSemantic = copyNamedWriteable(rewrittenSemantic, writableRegistry(), QueryBuilder.class);
        disableQueryInterception(
            queryRewriteContext,
            () -> assertCoordinatorNodeRewriteOnInferenceField(originalSemantic, serializedSemantic, transportVersion, queryRewriteContext)
        );

        // Query a field that is a semantic text field in one index
        QueryBuilder originalMixed = createQueryBuilder(mixedField);
        QueryBuilder rewrittenMixed = rewriteAndFetch(originalMixed, queryRewriteContext);
        QueryBuilder serializedMixed = copyNamedWriteable(rewrittenMixed, writableRegistry(), QueryBuilder.class);
        disableQueryInterception(
            queryRewriteContext,
            () -> assertCoordinatorNodeRewriteOnInferenceField(originalMixed, serializedMixed, transportVersion, queryRewriteContext)
        );

        // Query a text field in both indices
        QueryBuilder originalText = createQueryBuilder(textField);
        QueryBuilder rewrittenText = rewriteAndFetch(originalText, queryRewriteContext);
        QueryBuilder serializedText = copyNamedWriteable(rewrittenText, writableRegistry(), QueryBuilder.class);
        assertCoordinatorNodeRewriteOnNonInferenceField(originalText, serializedText);
    }

    protected QueryRewriteContext createQueryRewriteContext(
        Map<String, Map<String, String>> localIndexInferenceFields,
        Map<String, String> remoteIndexNames,
        TransportVersion minTransportVersion,
        Boolean ccsMinimizeRoundTrips
    ) {
        Map<Index, IndexMetadata> indexMetadata = new HashMap<>();
        for (var indexEntry : localIndexInferenceFields.entrySet()) {
            String indexName = indexEntry.getKey();
            Map<String, String> inferenceFields = localIndexInferenceFields.get(indexName);

            Index index = new Index(indexName, randomAlphaOfLength(10));
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
                .numberOfShards(1)
                .numberOfReplicas(0);

            for (var inferenceFieldEntry : inferenceFields.entrySet()) {
                String inferenceFieldName = inferenceFieldEntry.getKey();
                String inferenceId = inferenceFieldEntry.getValue();
                indexMetadataBuilder.putInferenceField(
                    new InferenceFieldMetadata(inferenceFieldName, inferenceId, new String[] { inferenceFieldName }, null)
                );
            }

            indexMetadata.put(index, indexMetadataBuilder.build());
        }

        Map<String, OriginalIndices> remoteIndices = new HashMap<>();
        if (remoteIndexNames != null) {
            for (var entry : remoteIndexNames.entrySet()) {
                remoteIndices.put(entry.getKey(), new OriginalIndices(new String[] { entry.getValue() }, IndicesOptions.DEFAULT));
            }
        }

        ResolvedIndices resolvedIndices = new MockResolvedIndices(
            remoteIndices,
            new OriginalIndices(localIndexInferenceFields.keySet().toArray(new String[0]), IndicesOptions.DEFAULT),
            indexMetadata
        );

        QueryRewriteInterceptor interceptor = createQueryRewriteInterceptor();
        Map<String, QueryRewriteInterceptor> interceptorMap = Map.of(interceptor.getQueryName(), interceptor);

        return new QueryRewriteContext(
            null,
            new MockInferenceClient(threadPool, INFERENCE_ENDPOINT_MAP),
            null,
            minTransportVersion,
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            null,
            QueryRewriteInterceptor.multi(interceptorMap),
            ccsMinimizeRoundTrips
        );
    }

    protected QueryRewriteContext createIndexMetadataContext(
        String indexName,
        Map<String, String> semanticTextFields,
        Map<String, Map<String, Object>> nonInferenceFields
    ) throws IOException {
        Client client = new NoOpClient(threadPool);

        Index index = new Index(indexName, randomAlphaOfLength(10));
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            )
            .numberOfShards(1)
            .numberOfReplicas(0);

        try (XContentBuilder mappings = XContentFactory.jsonBuilder()) {
            mappings.startObject().startObject("_doc").startObject("properties");

            for (var entry : semanticTextFields.entrySet()) {
                String fieldName = entry.getKey();
                String inferenceId = entry.getValue();
                MinimalServiceSettings modelSettings = INFERENCE_ENDPOINT_MAP.get(inferenceId);
                if (modelSettings == null) {
                    throw new IllegalArgumentException("No model settings for inference ID [" + inferenceId + "]");
                }

                mappings.startObject(fieldName);
                mappings.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
                mappings.field("inference_id", inferenceId);
                mappings.field("model_settings", modelSettings);
                mappings.endObject();
            }
            for (var entry : nonInferenceFields.entrySet()) {
                String fieldName = entry.getKey();
                Map<String, Object> properties = entry.getValue();

                mappings.startObject(fieldName);
                for (var propertyEntry : properties.entrySet()) {
                    mappings.field(propertyEntry.getKey(), propertyEntry.getValue());
                }
                mappings.endObject();
            }

            mappings.endObject().endObject().endObject();

            MapperService mapperService = createMapperService(mappings);
            MappingLookup mappingLookup = mapperService.mappingLookup();
            IndexSettings indexSettings = new IndexSettings(indexMetadataBuilder.build(), Settings.EMPTY);

            return new QueryRewriteContext(
                null,
                client,
                null,
                mapperService,
                mappingLookup,
                Map.of(),
                indexSettings,
                null,
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                index,
                pattern -> Regex.simpleMatch(pattern, index.getName()),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false
            );
        }
    }

    protected void assertRewriteAndSerializeOnInferenceField(
        QueryBuilder originalQuery,
        QueryRewriteContext queryRewriteContext,
        Exception expectedRewriteException,
        Exception expectedSerializationException
    ) throws IOException {
        if (expectedRewriteException != null) {
            Exception actualException = assertThrows(Exception.class, () -> rewriteAndFetch(originalQuery, queryRewriteContext));
            assertThat(actualException, instanceOf(expectedRewriteException.getClass()));
            assertThat(actualException.getMessage(), equalTo(expectedRewriteException.getMessage()));
            return;
        }
        QueryBuilder rewrittenQuery = rewriteAndFetch(originalQuery, queryRewriteContext);

        TransportVersion serializationTransportVersion = queryRewriteContext.getMinTransportVersion();
        if (expectedSerializationException != null) {
            Exception actualException = assertThrows(
                Exception.class,
                () -> copyNamedWriteable(rewrittenQuery, writableRegistry(), QueryBuilder.class, serializationTransportVersion)
            );
            assertThat(actualException, instanceOf(expectedSerializationException.getClass()));
            assertThat(actualException.getMessage(), equalTo(expectedSerializationException.getMessage()));
            return;
        }
        QueryBuilder serializedQuery = copyNamedWriteable(
            rewrittenQuery,
            writableRegistry(),
            QueryBuilder.class,
            serializationTransportVersion
        );

        // Disable query interception when checking the results of coordinator node rewrite so that the query rewrite context can be used
        // to populate inference results without triggering another query interception. In production this is achieved by wrapping with
        // InterceptedQueryBuilderWrapper, but we do not have access to that in this test.
        disableQueryInterception(
            queryRewriteContext,
            () -> assertCoordinatorNodeRewriteOnInferenceField(
                originalQuery,
                serializedQuery,
                queryRewriteContext.getMinTransportVersion(),
                queryRewriteContext
            )
        );
    }

    protected void assertRewriteAndSerializeOnNonInferenceField(QueryBuilder originalQuery, QueryRewriteContext queryRewriteContext)
        throws IOException {
        QueryBuilder rewrittenQuery = rewriteAndFetch(originalQuery, queryRewriteContext);
        QueryBuilder serializedQuery = copyNamedWriteable(
            rewrittenQuery,
            writableRegistry(),
            QueryBuilder.class,
            queryRewriteContext.getMinTransportVersion()
        );
        assertCoordinatorNodeRewriteOnNonInferenceField(originalQuery, serializedQuery);
    }

    protected static QueryBuilder rewriteAndFetch(QueryBuilder queryBuilder, QueryRewriteContext queryRewriteContext) {
        PlainActionFuture<QueryBuilder> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(queryBuilder, queryRewriteContext, future);
        return future.actionGet();
    }

    protected static void disableQueryInterception(QueryRewriteContext queryRewriteContext, Runnable runnable) {
        QueryRewriteInterceptor interceptor = queryRewriteContext.getQueryRewriteInterceptor();
        queryRewriteContext.setQueryRewriteInterceptor(null);
        runnable.run();
        queryRewriteContext.setQueryRewriteInterceptor(interceptor);
    }

    private static ModelRegistry createModelRegistry(ThreadPool threadPool) {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ModelRegistry modelRegistry = spy(new ModelRegistry(clusterService, new NoOpClient(threadPool)));
        modelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });

        doAnswer(i -> {
            String inferenceId = i.getArgument(0);
            MinimalServiceSettings settings = INFERENCE_ENDPOINT_MAP.get(inferenceId);
            if (settings == null) {
                throw new ResourceNotFoundException(inferenceId + " does not exist");
            }
            return settings;
        }).when(modelRegistry).getMinimalServiceSettings(anyString());

        return modelRegistry;
    }

    protected record TestIndex(String name, Map<String, String> semanticTextFields, Map<String, Map<String, Object>> nonInferenceFields) {}
}
