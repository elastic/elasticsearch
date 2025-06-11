/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.apache.lucene.search.BooleanClause.Occur.SHOULD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class SemanticQueryBuilderTests extends AbstractQueryTestCase<SemanticQueryBuilder> {
    private static final String SEMANTIC_TEXT_FIELD = "semantic";
    private static final float TOKEN_WEIGHT = 0.5f;
    private static final int QUERY_TOKEN_LENGTH = 4;
    private static final int TEXT_EMBEDDING_DIMENSION_COUNT = 16; // Must be a multiple of 8 to be a valid bit embedding
    private static final String INFERENCE_ID = "test_service";
    private static final String SEARCH_INFERENCE_ID = "search_test_service";

    private static InferenceResultType inferenceResultType;
    private static DenseVectorFieldMapper.ElementType denseVectorElementType;
    private static boolean useSearchInferenceId;
    private final boolean useLegacyFormat;

    private enum InferenceResultType {
        NONE,
        SPARSE_EMBEDDING,
        TEXT_EMBEDDING
    }

    private Integer queryTokenCount;

    public SemanticQueryBuilderTests(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @BeforeClass
    public static void setInferenceResultType() {
        // These are class variables because they are used when initializing additional mappings, which happens once per test suite run in
        // AbstractBuilderTestCase#beforeTest as part of service holder creation.
        inferenceResultType = randomFrom(InferenceResultType.values());
        denseVectorElementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
        useSearchInferenceId = randomBoolean();
    }

    @BeforeClass
    public static void startModelRegistry() {
        threadPool = new TestThreadPool(SemanticQueryBuilderTests.class.getName());
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool));
        modelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
    }

    @AfterClass
    public static void stopModelRegistry() {
        IOUtils.closeWhileHandlingException(threadPool);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        queryTokenCount = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(XPackClientPlugin.class, InferencePluginWithModelRegistry.class, FakeMlPlugin.class);
    }

    @Override
    protected Settings createTestIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        String mappingConfig = "type=semantic_text,inference_id=" + INFERENCE_ID;
        if (useSearchInferenceId) {
            mappingConfig += ",search_inference_id=" + SEARCH_INFERENCE_ID;
        }

        mapperService.merge(
            "_doc",
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(SEMANTIC_TEXT_FIELD, mappingConfig))),
            MapperService.MergeReason.MAPPING_UPDATE
        );

        applyRandomInferenceResults(mapperService);
    }

    private void applyRandomInferenceResults(MapperService mapperService) throws IOException {
        // Parse random inference results (or no inference results) to set up the dynamic inference result mappings under the semantic text
        // field
        SourceToParse sourceToParse = buildSemanticTextFieldWithInferenceResults(
            inferenceResultType,
            denseVectorElementType,
            useLegacyFormat
        );
        if (sourceToParse != null) {
            ParsedDocument parsedDocument = mapperService.documentMapper().parse(sourceToParse);
            mapperService.merge(
                "_doc",
                parsedDocument.dynamicMappingsUpdate().toCompressedXContent(),
                MapperService.MergeReason.MAPPING_UPDATE
            );
        }
    }

    @Override
    protected SemanticQueryBuilder doCreateTestQueryBuilder() {
        queryTokenCount = randomIntBetween(1, 5);
        List<String> queryTokens = new ArrayList<>(queryTokenCount);
        for (int i = 0; i < queryTokenCount; i++) {
            queryTokens.add(randomAlphaOfLength(QUERY_TOKEN_LENGTH));
        }

        SemanticQueryBuilder builder = new SemanticQueryBuilder(SEMANTIC_TEXT_FIELD, String.join(" ", queryTokens));
        if (randomBoolean()) {
            builder.boost((float) randomDoubleBetween(0.1, 10.0, true));
        }
        if (randomBoolean()) {
            builder.queryName(randomAlphaOfLength(4));
        }

        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(SemanticQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(queryTokenCount, notNullValue());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(ESToParentBlockJoinQuery.class));

        ESToParentBlockJoinQuery nestedQuery = (ESToParentBlockJoinQuery) query;
        assertThat(nestedQuery.getScoreMode(), equalTo(ScoreMode.Max));

        switch (inferenceResultType) {
            case NONE -> assertThat(nestedQuery.getChildQuery(), instanceOf(MatchNoDocsQuery.class));
            case SPARSE_EMBEDDING -> assertSparseEmbeddingLuceneQuery(nestedQuery.getChildQuery());
            case TEXT_EMBEDDING -> assertTextEmbeddingLuceneQuery(nestedQuery.getChildQuery());
        }
    }

    private void assertSparseEmbeddingLuceneQuery(Query query) {
        Query innerQuery = assertOuterBooleanQuery(query);
        assertThat(innerQuery, instanceOf(SparseVectorQueryWrapper.class));
        var sparseQuery = (SparseVectorQueryWrapper) innerQuery;
        assertThat(sparseQuery.getTermsQuery(), instanceOf(BooleanQuery.class));

        BooleanQuery innerBooleanQuery = (BooleanQuery) sparseQuery.getTermsQuery();
        assertThat(innerBooleanQuery.clauses().size(), equalTo(queryTokenCount));
        innerBooleanQuery.forEach(c -> {
            assertThat(c.getOccur(), equalTo(SHOULD));
            assertThat(c.getQuery(), instanceOf(BoostQuery.class));
            assertThat(((BoostQuery) c.getQuery()).getBoost(), equalTo(TOKEN_WEIGHT));
        });
    }

    private void assertTextEmbeddingLuceneQuery(Query query) {
        Query innerQuery = assertOuterBooleanQuery(query);

        Class<? extends Query> expectedKnnQueryClass = switch (denseVectorElementType) {
            case FLOAT -> KnnFloatVectorQuery.class;
            case BYTE, BIT -> KnnByteVectorQuery.class;
        };
        assertThat(innerQuery, instanceOf(expectedKnnQueryClass));
    }

    private Query assertOuterBooleanQuery(Query query) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery outerBooleanQuery = (BooleanQuery) query;

        List<BooleanClause> outerMustClauses = new ArrayList<>();
        List<BooleanClause> outerFilterClauses = new ArrayList<>();
        for (BooleanClause clause : outerBooleanQuery.clauses()) {
            BooleanClause.Occur occur = clause.getOccur();
            if (occur == MUST) {
                outerMustClauses.add(clause);
            } else if (occur == FILTER) {
                outerFilterClauses.add(clause);
            } else {
                fail("Unexpected boolean " + occur + " clause");
            }
        }

        assertThat(outerMustClauses.size(), equalTo(1));
        assertThat(outerFilterClauses.size(), equalTo(1));

        return outerMustClauses.get(0).getQuery();
    }

    @Override
    protected boolean canSimulateMethod(Method method, Object[] args) throws NoSuchMethodException {
        return method.equals(Client.class.getMethod("execute", ActionType.class, ActionRequest.class, ActionListener.class))
            && (args[0] instanceof InferenceAction);
    }

    @Override
    protected Object simulateMethod(Method method, Object[] args) {
        InferenceAction.Request request = (InferenceAction.Request) args[1];
        assertThat(request.getTaskType(), equalTo(TaskType.ANY));
        assertThat(request.getInputType(), equalTo(InputType.INTERNAL_SEARCH));
        assertThat(request.getInferenceEntityId(), equalTo(useSearchInferenceId ? SEARCH_INFERENCE_ID : INFERENCE_ID));

        List<String> input = request.getInput();
        assertThat(input.size(), equalTo(1));
        String query = input.get(0);

        InferenceAction.Response response = switch (inferenceResultType) {
            case NONE -> randomBoolean() ? generateSparseEmbeddingInferenceResponse(query) : generateTextEmbeddingInferenceResponse();
            case SPARSE_EMBEDDING -> generateSparseEmbeddingInferenceResponse(query);
            case TEXT_EMBEDDING -> generateTextEmbeddingInferenceResponse();
        };

        @SuppressWarnings("unchecked")  // We matched the method above.
        ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) args[2];
        listener.onResponse(response);

        return null;
    }

    private InferenceAction.Response generateSparseEmbeddingInferenceResponse(String query) {
        List<WeightedToken> weightedTokens = Arrays.stream(query.split("\\s+")).map(s -> new WeightedToken(s, TOKEN_WEIGHT)).toList();
        TextExpansionResults textExpansionResults = new TextExpansionResults(DEFAULT_RESULTS_FIELD, weightedTokens, false);

        return new InferenceAction.Response(SparseEmbeddingResults.of(List.of(textExpansionResults)));
    }

    private InferenceAction.Response generateTextEmbeddingInferenceResponse() {
        int inferenceLength = DenseVectorFieldMapperTestUtils.getEmbeddingLength(denseVectorElementType, TEXT_EMBEDDING_DIMENSION_COUNT);
        double[] inference = new double[inferenceLength];
        Arrays.fill(inference, 1.0);
        MlTextEmbeddingResults textEmbeddingResults = new MlTextEmbeddingResults(DEFAULT_RESULTS_FIELD, inference, false);

        return new InferenceAction.Response(TextEmbeddingFloatResults.of(List.of(textEmbeddingResults)));
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        SemanticQueryBuilder builder = new SemanticQueryBuilder("foo", "bar");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builder.toQuery(context));
        assertThat(e.getMessage(), equalTo(SemanticQueryBuilder.NAME + " should have been rewritten to another query type"));
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SemanticQueryBuilder(null, "query"));
            assertThat(e.getMessage(), equalTo("[semantic] requires a field value"));
        }
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SemanticQueryBuilder("fieldName", null));
            assertThat(e.getMessage(), equalTo("[semantic] requires a query value"));
        }
    }

    public void testToXContent() throws IOException {
        QueryBuilder queryBuilder = new SemanticQueryBuilder("foo", "bar");
        checkGeneratedJson("""
            {
              "semantic": {
                "field": "foo",
                "query": "bar"
              }
            }""", queryBuilder);
    }

    public void testSerializingQueryWhenNoInferenceId() throws IOException {
        // Test serializing the query after rewriting on the coordinator node when no inference ID could be resolved for the field
        SemanticQueryBuilder builder = new SemanticQueryBuilder(SEMANTIC_TEXT_FIELD + "_missing", "query text");

        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        queryRewriteContext.setAllowUnmappedFields(true);

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        searchExecutionContext.setAllowUnmappedFields(true);

        QueryBuilder rewritten = rewriteQuery(builder, queryRewriteContext, searchExecutionContext);
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
    }

    private static SourceToParse buildSemanticTextFieldWithInferenceResults(
        InferenceResultType inferenceResultType,
        DenseVectorFieldMapper.ElementType denseVectorElementType,
        boolean useLegacyFormat
    ) throws IOException {
        var modelSettings = switch (inferenceResultType) {
            case NONE -> null;
            case SPARSE_EMBEDDING -> new MinimalServiceSettings("my-service", TaskType.SPARSE_EMBEDDING, null, null, null);
            case TEXT_EMBEDDING -> new MinimalServiceSettings(
                "my-service",
                TaskType.TEXT_EMBEDDING,
                TEXT_EMBEDDING_DIMENSION_COUNT,
                // l2_norm similarity is required for bit embeddings
                denseVectorElementType == DenseVectorFieldMapper.ElementType.BIT ? SimilarityMeasure.L2_NORM : SimilarityMeasure.COSINE,
                denseVectorElementType
            );
        };

        SourceToParse sourceToParse = null;
        if (modelSettings != null) {
            SemanticTextField semanticTextField = new SemanticTextField(
                useLegacyFormat,
                SEMANTIC_TEXT_FIELD,
                null,
                new SemanticTextField.InferenceResult(INFERENCE_ID, modelSettings, null, Map.of(SEMANTIC_TEXT_FIELD, List.of())),
                XContentType.JSON
            );

            XContentBuilder builder = JsonXContent.contentBuilder().startObject();
            if (useLegacyFormat == false) {
                builder.startObject(InferenceMetadataFieldsMapper.NAME);
            }
            builder.field(semanticTextField.fieldName(), semanticTextField);
            if (useLegacyFormat == false) {
                builder.endObject();
            }
            builder.endObject();
            sourceToParse = new SourceToParse("test", BytesReference.bytes(builder), XContentType.JSON);
        }

        return sourceToParse;
    }

    public static class FakeMlPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }
    }

    private static TestThreadPool threadPool;
    private static ModelRegistry modelRegistry;

    public static class InferencePluginWithModelRegistry extends InferencePlugin {
        public InferencePluginWithModelRegistry(Settings settings) {
            super(settings);
        }

        @Override
        protected Supplier<ModelRegistry> getModelRegistry() {
            return () -> modelRegistry;
        }
    }
}
