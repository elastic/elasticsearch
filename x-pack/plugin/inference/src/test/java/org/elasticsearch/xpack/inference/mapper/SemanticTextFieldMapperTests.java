/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EndpointClusterState;
import org.elasticsearch.inference.EndpointMetadataTests;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.inference.metadata.EndpointMetadataClusterState;
import org.elasticsearch.license.License;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests.randomIndexOptionsAll;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldTypeTests.randomSparseVectorIndexOptions;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_EIS_ELSER_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_EIS_JINA_V5_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_FALLBACK_ELSER_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_RESCORE_OVERSAMPLE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.INDEX_OPTIONS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.UNSUPPORTED_INDEX_MESSAGE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettingsOtherThan;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomMultimodalEmbedding;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SemanticTextFieldMapperTests extends AbstractSemanticMapperTestCase<
    SemanticTextFieldMapper,
    SemanticTextFieldMapper.SemanticTextFieldType> {

    private static final TestModel JINA_V5_TEXT_MODEL = new TestModel(
        DEFAULT_EIS_JINA_V5_INFERENCE_ID,
        TaskType.TEXT_EMBEDDING,
        ElasticInferenceService.NAME,
        new TestModel.TestServiceSettings("jina", 1024, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
        new TestModel.TestTaskSettings((Integer) null),
        new TestModel.TestSecretSettings("foo")
    );
    private static final TestModel DEFAULT_EIS_ELSER_MODEL = new TestModel(
        DEFAULT_EIS_ELSER_INFERENCE_ID,
        TaskType.SPARSE_EMBEDDING,
        ElasticInferenceService.NAME,
        new TestModel.TestServiceSettings("elser", null, null, null),
        new TestModel.TestTaskSettings((Integer) null),
        new TestModel.TestSecretSettings("foo")
    );
    private static final TestModel DEFAULT_FALLBACK_ELSER_MODEL = new TestModel(
        DEFAULT_FALLBACK_ELSER_INFERENCE_ID,
        TaskType.SPARSE_EMBEDDING,
        ElasticsearchInternalService.NAME,
        new TestModel.TestServiceSettings("elser", null, null, null),
        new TestModel.TestTaskSettings((Integer) null),
        new TestModel.TestSecretSettings("foo")
    );

    private static final List<TestModel> DEFAULT_MODELS = List.of(
        JINA_V5_TEXT_MODEL,
        DEFAULT_EIS_ELSER_MODEL,
        DEFAULT_FALLBACK_ELSER_MODEL
    );

    private final boolean useLegacyFormat;

    private final IndexVersion testIndexVersion;

    @SuppressWarnings("this-escape")
    public SemanticTextFieldMapperTests(boolean useLegacyFormat, License.OperationMode operationMode) {
        super(operationMode);
        this.useLegacyFormat = useLegacyFormat;
        this.testIndexVersion = useLegacyFormat
            ? SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(true)
            : super.getVersion();
    }

    @Override
    protected void registerDefaultEndpoints() {
        for (TestModel model : DEFAULT_MODELS) {
            globalModelRegistry.putDefaultIdIfAbsent(
                new InferenceService.DefaultConfigId(
                    model.getInferenceEntityId(),
                    new EndpointClusterState(model),
                    mock(InferenceService.class)
                )
            );
        }
    }

    private void removeDefaultEndpoints(Set<String> inferenceIds) {
        PlainActionFuture<Boolean> removalFuture = new PlainActionFuture<>();
        globalModelRegistry.removeDefaultConfigs(inferenceIds, removalFuture);
        safeGet(removalFuture);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(
            new Object[] { true, License.OperationMode.BASIC },
            new Object[] { true, License.OperationMode.ENTERPRISE },
            new Object[] { false, License.OperationMode.BASIC },
            new Object[] { false, License.OperationMode.ENTERPRISE }
        );
    }

    @Override
    protected String minimalIsInvalidRoutingPathErrorMessage(Mapper mapper) {
        assumeFalse("invalid routing path error message is only checked for mappers created with the new format", useLegacyFormat);

        return super.minimalIsInvalidRoutingPathErrorMessage(mapper);
    }

    @Override
    protected boolean useLegacyFormat() {
        return useLegacyFormat;
    }

    @Override
    protected IndexVersion getVersion() {
        return testIndexVersion;
    }

    @Override
    protected Settings getIndexSettings() {
        if (useLegacyFormat) {
            return Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), testIndexVersion)
                .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true)
                .build();
        }
        return super.getIndexSettings();
    }

    /**
     * Restricts the set of index versions tested by {@link #testSupportedIndexVersions()} to those
     * that are compatible with the current {@code useLegacyFormat} value.  When the legacy format
     * is enabled the index version must be strictly before
     * {@link IndexVersions#SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN}; all released versions are
     * valid when the legacy format is disabled.
     */
    @Override
    protected Set<IndexVersion> getSupportedVersions() {
        if (useLegacyFormat) {
            return IndexVersionUtils.allReleasedVersions()
                .stream()
                .filter(
                    v -> v.onOrAfter(IndexVersions.SEMANTIC_TEXT_FIELD_TYPE)
                        && v.before(IndexVersions.SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN)
                )
                .collect(Collectors.toSet());
        }
        return super.getSupportedVersions();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "semantic_text");
    }

    @Override
    protected void metaMapping(XContentBuilder b) throws IOException {
        super.metaMapping(b);
        b.field(INFERENCE_ID_FIELD, DEFAULT_EIS_JINA_V5_INFERENCE_ID);
    }

    @Override
    protected Object getSampleObjectForDocument() {
        // Only consulted by testSupportsParsingObject, and only for the legacy format (supportsParsingObject() is true only then). A
        // legacy value is the {text, inference} object; a minimal one with the resolved inference id and no inference results parses.
        final String expectedInferenceId = testIndexVersion.onOrAfter(IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5)
            ? DEFAULT_EIS_JINA_V5_INFERENCE_ID
            : DEFAULT_EIS_ELSER_INFERENCE_ID;
        return Map.of(
            SemanticTextField.TEXT_FIELD,
            randomAlphaOfLength(10),
            SemanticTextField.INFERENCE_FIELD,
            Map.of(SemanticTextField.INFERENCE_ID_FIELD, expectedInferenceId, SemanticTextField.CHUNKS_FIELD, List.of())
        );
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new SemanticTextFieldMapper.SemanticTextFieldType(
            "field",
            "fake-inference-id",
            null,
            null,
            null,
            null,
            null,
            false,
            false,
            Map.of()
        );
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        assertTrue(fieldType.isSearchable());
    }

    @Override
    protected DenseVectorFieldMapper.DenseVectorIndexOptions getExplicitDenseVectorIndexOptions(
        EndpointClusterState modelSettings,
        IndexVersion indexVersion
    ) {
        TaskType taskType = modelSettings.taskType();
        DenseVectorFieldMapper.ElementType elementType = modelSettings.elementType();
        Integer dimensions = modelSettings.dimensions();

        if (taskType == TaskType.SPARSE_EMBEDDING) {
            return null;
        }

        DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions = null;
        boolean floatFamilyElementType = elementType == DenseVectorFieldMapper.ElementType.FLOAT
            || elementType == DenseVectorFieldMapper.ElementType.BFLOAT16;
        if (floatFamilyElementType
            && SemanticTextFieldMapper.setExplicitIndexOptionsForSemanticText(indexVersion)
            && dimensions >= DenseVectorFieldMapper.BBQ_MIN_DIMS) {
            indexOptions = defaultBbqHnswDenseVectorIndexOptions();
        }

        return indexOptions;
    }

    /**
     * Randomized sweep over all combinations of endpoint availability and
     * index version to confirm correct default inference ID.
     */
    public void testDefaults() throws Exception {
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
        final XContentBuilder expectedMapping = fieldMapping(this::metaMapping);

        MapperService mapperService = createSemanticMapperService(fieldMapping, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5);
        DocumentMapper mapper = mapperService.documentMapper();
        assertEquals(Strings.toString(expectedMapping), mapper.mappingSource().toString());
        assertSemanticField(mapperService, fieldName, false, new EndpointClusterState(JINA_V5_TEXT_MODEL), null, null);
        assertInferenceEndpoints(mapperService, fieldName, DEFAULT_EIS_JINA_V5_INFERENCE_ID, DEFAULT_EIS_JINA_V5_INFERENCE_ID);

        ParsedDocument doc1 = mapper.parse(source(this::writeField));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        // No indexable fields
        assertTrue(fields.isEmpty());

        for (int i = 0; i < 20; i++) {
            registerDefaultEndpoints();

            boolean jinaAvailable = randomBoolean();
            boolean eisElserAvailable = randomBoolean();
            boolean postJinaV5 = randomBoolean();

            final IndexVersion indexVersion;
            if (postJinaV5) {
                indexVersion = IndexVersionUtils.randomVersionBetween(
                    IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5,
                    useLegacyFormat
                        ? IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN)
                        : IndexVersion.current()
                );
            } else if (useLegacyFormat) {
                // Legacy format: any write-compatible version below JinaV5
                indexVersion = IndexVersionUtils.randomVersionBetween(
                    IndexVersions.MINIMUM_COMPATIBLE,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5)
                );
            } else if (randomBoolean()) {
                // New format, 9.x, pre-JinaV5
                indexVersion = IndexVersionUtils.randomVersionBetween(
                    IndexVersions.INFERENCE_METADATA_FIELDS,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5)
                );
            } else {
                // New format, 8.x backport window
                indexVersion = IndexVersionUtils.randomVersionBetween(
                    IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT,
                    IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                );
            }

            Set<String> unavailableEndpoints = new HashSet<>();
            if (jinaAvailable == false) {
                unavailableEndpoints.add(DEFAULT_EIS_JINA_V5_INFERENCE_ID);
            }
            if (eisElserAvailable == false) {
                unavailableEndpoints.add(DEFAULT_EIS_ELSER_INFERENCE_ID);
            }
            removeDefaultEndpoints(unavailableEndpoints);

            TestModel expectedModel;
            if (jinaAvailable && postJinaV5) {
                expectedModel = JINA_V5_TEXT_MODEL;
            } else if (eisElserAvailable) {
                expectedModel = DEFAULT_EIS_ELSER_MODEL;
            } else {
                expectedModel = DEFAULT_FALLBACK_ELSER_MODEL;
            }

            MapperService iterMapperService = createSemanticMapperServiceWithIndexVersion(fieldMapping, indexVersion);
            assertSemanticField(iterMapperService, fieldName, false, new EndpointClusterState(expectedModel), null, null);
            assertInferenceEndpoints(
                iterMapperService,
                fieldName,
                expectedModel.getInferenceEntityId(),
                expectedModel.getInferenceEntityId()
            );
        }
    }

    /**
     * The original input value of a {@code semantic_text} field is stored in an internal binary doc values field so that
     * {@code _source} can be rebuilt from doc values alone. This verifies the round-trip for a single value, multiple values
     * (order and duplicates preserved), and that the internal {@code inference} subtree does not leak into the rebuilt source.
     */
    public void testOriginalValueRoundTripFromDocValues() throws IOException {
        assumeFalse("the legacy format keeps the original value in _source", useLegacyFormat);
        MapperService mapperService = createSemanticMapperServiceWithSourceMode(
            fieldMapping(this::minimalMapping),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        DocumentMapper mapper = mapperService.documentMapper();

        assertThat(syntheticSource(mapper, b -> b.field("field", "some text")), equalTo("{\"field\":\"some text\"}"));
        // Document order and duplicates are preserved for multi-valued fields.
        assertThat(syntheticSource(mapper, b -> b.array("field", "b", "a", "b")), equalTo("{\"field\":[\"b\",\"a\",\"b\"]}"));
        // A single-element array is rebuilt as a scalar (standard synthetic-source normalization, not specific to this field).
        assertThat(syntheticSource(mapper, b -> b.array("field", "only")), equalTo("{\"field\":\"only\"}"));
        // A null value leaves the field absent from the rebuilt source.
        assertThat(syntheticSource(mapper, b -> b.nullField("field")), equalTo("{}"));
    }

    /**
     * The internal binary doc values store for the original input is only written for indices created on or after
     * {@link IndexVersions#SEMANTIC_TEXT_ORIGINAL_VALUES_DOC_VALUES}; older indices keep the released behavior (original value in
     * {@code _source} / {@code _ignored_source}).
     */
    public void testOriginalValuesStoredOnlyForNewIndices() throws IOException {
        assumeFalse("the legacy format keeps the original value in _source", useLegacyFormat);
        final String dvFieldName = SemanticTextField.getOriginalValuesFieldName("field");

        MapperService current = createSemanticMapperServiceWithSourceMode(
            fieldMapping(this::minimalMapping),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        ParsedDocument currentDoc = current.documentMapper().parse(source(b -> b.field("field", "hello")));
        assertNotNull("new indices store the original value in doc values", currentDoc.rootDoc().getField(dvFieldName));

        IndexVersion previous = IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_ORIGINAL_VALUES_DOC_VALUES);
        MapperService old = createSemanticMapperServiceWithSourceMode(
            fieldMapping(this::minimalMapping),
            previous,
            SourceFieldMapper.Mode.SYNTHETIC
        );
        ParsedDocument oldDoc = old.documentMapper().parse(source(b -> b.field("field", "hello")));
        assertNull("pre-feature indices do not store the original value in doc values", oldDoc.rootDoc().getField(dvFieldName));
    }

    /**
     * Columnar index modes reject any field whose {@code _source} is not reconstructable from doc values. A {@code semantic_text}
     * field is accepted because its original value is stored in doc values and its internal {@code inference} sub-fields (e.g.
     * {@code offset_source}) are exempt - they are not part of {@code _source} but are reconstructed into {@code _inference_fields}.
     * The value round-trip itself is covered by {@link #testOriginalValueRoundTripFromDocValues()} (the loader is identical).
     */
    public void testSemanticTextAcceptedInColumnar() throws IOException {
        assumeFalse("the legacy format keeps the original value in _source", useLegacyFormat);
        final String dvFieldName = SemanticTextField.getOriginalValuesFieldName("field");
        // Deliberately use an index version BEFORE the synthetic-source gate: in columnar mode the input is always stored in doc
        // values regardless of index version (columnar is unreleased), so the version gate applies only to non-columnar synthetic
        // source. This verifies columnar acceptance does not depend on the index version.
        final IndexVersion indexVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_ORIGINAL_VALUES_DOC_VALUES);
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            // Mapping creation succeeding is the assertion: the columnar "every field reconstructable from doc values" check passes.
            MapperService mapperService = createSemanticMapperServiceWithIndexMode(
                fieldMapping(this::minimalMapping),
                indexVersion,
                indexMode
            );
            ParsedDocument doc = mapperService.documentMapper()
                .parse(source(b -> b.field("@timestamp", "2024-01-01T00:00:00Z").field("field", "some text")));
            assertNotNull("original value stored in doc values for columnar _source reconstruction", doc.rootDoc().getField(dvFieldName));
        }
    }

    /**
     * When {@code _source} is reconstructed from doc values, retrieving the field (the {@code fields} option, highlighting) reads the
     * original value from its binary doc values column rather than {@code _source}. This verifies the value fetcher reads the doc
     * values (order and duplicates preserved) and declares no {@code _source} requirement, so the fetch phase need not rebuild it.
     */
    public void testOriginalValueFetchedFromDocValues() throws IOException {
        assumeFalse("the legacy format keeps the original value in _source", useLegacyFormat);
        MapperService mapperService = createSemanticMapperServiceWithSourceMode(
            fieldMapping(this::minimalMapping),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.array("field", "alpha", "beta", "alpha")));

        SearchExecutionContext searchContext = createSearchExecutionContext(mapperService);
        ValueFetcher fetcher = searchContext.getFieldType("field").valueFetcher(searchContext, null);
        assertThat(fetcher, instanceOf(OriginalValuesDocValuesFetcher.class));
        assertThat(fetcher.storedFieldsSpec(), equalTo(StoredFieldsSpec.NO_REQUIREMENTS));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            LeafReaderContext leaf = reader.leaves().get(0);
            fetcher.setNextReader(leaf);
            // An empty Source proves the values come from doc values and not from _source.
            List<Object> values = fetcher.fetchValues(Source.empty(XContentType.JSON), 0, new ArrayList<>());
            assertThat(values, contains("alpha", "beta", "alpha"));
        });
    }

    /**
     * The semantic highlighter declares it can highlight without {@code _source} only when every inference source field is
     * retrievable from doc values, so the fetch phase does not rebuild {@code _source} for it. When the original value is kept in
     * {@code _source} (stored mode), it must still load {@code _source}.
     */
    public void testHighlighterAvoidsSourceWhenValuesInDocValues() throws IOException {
        assumeFalse("the legacy format keeps the original value in _source", useLegacyFormat);
        SemanticTextHighlighter highlighter = new SemanticTextHighlighter();

        MapperService synthetic = createSemanticMapperServiceWithSourceMode(
            fieldMapping(this::minimalMapping),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        SearchExecutionContext syntheticContext = createSearchExecutionContext(synthetic);
        assertTrue(highlighter.canHighlightWithoutSource(syntheticContext.getFieldType("field"), syntheticContext));

        MapperService stored = createSemanticMapperServiceWithSourceMode(
            fieldMapping(this::minimalMapping),
            IndexVersion.current(),
            SourceFieldMapper.Mode.STORED
        );
        SearchExecutionContext storedContext = createSearchExecutionContext(stored);
        assertFalse(highlighter.canHighlightWithoutSource(storedContext.getFieldType("field"), storedContext));
    }

    /**
     * When a {@code semantic_text} field is fed by {@code copy_to} from a plain text field, the highlighter also loads that text
     * field. Since it is only retrievable from {@code _source}, the highlighter cannot skip {@code _source} even in synthetic source.
     */
    public void testHighlighterRequiresSourceWhenCopyToSourceFieldNeedsIt() throws IOException {
        assumeFalse("the legacy format keeps the original value in _source", useLegacyFormat);
        MapperService mapperService = createSemanticMapperServiceWithSourceMode(mapping(b -> {
            b.startObject("body").field("type", "text").field("copy_to", "field").endObject();
            addSemanticMapping(b, "field", "test_model", null, null, null, null);
        }), IndexVersion.current(), SourceFieldMapper.Mode.SYNTHETIC);
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        assertFalse(new SemanticTextHighlighter().canHighlightWithoutSource(context.getFieldType("field"), context));
    }

    public void testIndexSettingWithCustomInferenceId() throws Exception {
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
        final String customEndpoint = "my-custom-elser-endpoint";

        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .put(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey(), customEndpoint)
            .build();
        MapperService mapperService = createMapperService(indexVersion, settings, fieldMapping);
        assertInferenceEndpoints(mapperService, fieldName, customEndpoint, customEndpoint);
    }

    public void testExplicitFieldInferenceIdTakesPrecedenceOverIndexSetting() throws Exception {
        final String fieldName = "field";
        final String explicitEndpoint = "explicit-endpoint";
        final XContentBuilder fieldMapping = semanticMapping("field", explicitEndpoint);

        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .put(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey(), "my-custom-endpoint")
            .build();
        MapperService mapperService = createMapperService(indexVersion, settings, fieldMapping);
        assertInferenceEndpoints(mapperService, fieldName, explicitEndpoint, explicitEndpoint);
    }

    public void testEmptyDefaultInferenceIdSettingThrows() throws Exception {
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);
        for (String blank : new String[] { null, "", " ", "   " }) {
            var settings = Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
                .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
                .put(SemanticTextFieldMapper.INDEX_SEMANTIC_TEXT_DEFAULT_INFERENCE_ID.getKey(), blank)
                .build();
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(indexVersion, settings, fieldMapping));
            assertThat(e.getMessage(), containsString("[index.semantic_text.default_inference_id] must not be blank"));
        }
    }

    @Override
    public void testFieldHasValue() {
        MappedFieldType fieldType = getMappedFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName(getEmbeddingsFieldName("field")) });
        assertTrue(fieldType.fieldHasValue(fieldInfos));
    }

    public void testSetInferenceEndpoints() throws IOException {
        final String fieldName = "field";
        final String inferenceId = "foo";
        final String searchInferenceId = "bar";

        CheckedBiConsumer<XContentBuilder, MapperService, IOException> assertSerialization = (expectedMapping, mapperService) -> {
            DocumentMapper mapper = mapperService.documentMapper();
            assertEquals(Strings.toString(expectedMapping), mapper.mappingSource().toString());
        };

        {
            final XContentBuilder fieldMapping = semanticMapping("field", inferenceId);
            final MapperService mapperService = createSemanticMapperService(fieldMapping);
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
            assertSerialization.accept(fieldMapping, mapperService);
        }
        {
            final XContentBuilder fieldMapping = semanticMapping("field", null, searchInferenceId);
            final XContentBuilder expectedMapping = semanticMapping("field", DEFAULT_EIS_JINA_V5_INFERENCE_ID, searchInferenceId);
            final MapperService mapperService = createSemanticMapperService(fieldMapping, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_JINA_V5);
            assertSemanticField(mapperService, fieldName, false, new EndpointClusterState(JINA_V5_TEXT_MODEL), null, null);
            assertInferenceEndpoints(mapperService, fieldName, DEFAULT_EIS_JINA_V5_INFERENCE_ID, searchInferenceId);
            assertSerialization.accept(expectedMapping, mapperService);
        }
        {
            final XContentBuilder fieldMapping = semanticMapping("field", inferenceId, searchInferenceId);
            MapperService mapperService = createSemanticMapperService(fieldMapping);
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId);
            assertSerialization.accept(fieldMapping, mapperService);
        }
    }

    public void testInvalidInferenceEndpoints() {
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createSemanticMapperService(
                    fieldMapping(b -> b.field("type", "semantic_text").field(INFERENCE_ID_FIELD, (String) null))
                )
            );
            assertThat(
                e.getMessage(),
                containsString("[inference_id] on mapper [field] of type [semantic_text] must not have a [null] value")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createSemanticMapperService(semanticMapping("field", "")));
            assertThat(e.getMessage(), containsString("[inference_id] on mapper [field] of type [semantic_text] must not be empty"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createSemanticMapperService(semanticMapping("field", null, "")));
            assertThat(e.getMessage(), containsString("[search_inference_id] on mapper [field] of type [semantic_text] must not be empty"));
        }
    }

    public void testInvalidTaskTypes() {
        for (var taskType : TaskType.values()) {
            if (taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.SPARSE_EMBEDDING || taskType == TaskType.EMBEDDING) {
                continue;
            }
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createSemanticMapperService(semanticMapping("field", "test1", null, null, null, null, b -> {
                    b.startObject("model_settings");
                    b.field("task_type", taskType);
                    b.endObject();
                }))
            );
            assertThat(e.getMessage(), containsString("Wrong [task_type], expected text_embedding, embedding, or sparse_embedding"));
        }
    }

    public void testMultiFieldNamedInputIsRejected() throws IOException {
        // The new format stores the original input in an internal [<field>.input] doc values column; a multi-field with that name
        // would write to the same Lucene field, so it is rejected. (Multi-fields are rejected outright in the legacy format.)
        assumeFalse("multi-fields are rejected entirely in the legacy format", useLegacyFormat);
        IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(
            IndexVersions.SEMANTIC_TEXT_ORIGINAL_VALUES_DOC_VALUES,
            IndexVersion.current()
        );
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(semanticMapping("field", "my_inference_id", null, null, null, null, b -> {
                b.startObject("fields");
                b.startObject("input").field("type", "keyword").endObject();
                b.endObject();
            }), indexVersion)
        );
        assertThat(e.getMessage(), containsString("cannot have a multi-field named [input]"));
    }

    public void testMultiFieldNamedInputIsAccepted() throws IOException {
        // At an index version before the original values are stored in doc values, the [<field>.input] column is not reserved, so
        // a multi-field named [input] stays valid. This verifies the gating does not invalidate indices created at an older version.
        assumeFalse("multi-fields are rejected entirely in the legacy format", useLegacyFormat);
        IndexVersion indexVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_ORIGINAL_VALUES_DOC_VALUES);
        MapperService mapperService = createSemanticMapperServiceWithIndexVersion(
            semanticMapping("field", "my_inference_id", null, null, null, null, b -> {
                b.startObject("fields");
                b.startObject("input").field("type", "keyword").endObject();
                b.endObject();
            }),
            indexVersion
        );
        assertNotNull(mapperService.mappingLookup().getMapper("field.input"));
    }

    public void testMultiFieldsSupport() throws IOException {
        if (useLegacyFormat) {
            Exception e = expectThrows(MapperParsingException.class, () -> createSemanticMapperService(fieldMapping(b -> {
                b.field("type", "text");
                b.startObject("fields");
                addSemanticMapping(b, "semantic", "my_inference_id", null, null, null, null);
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("Field [semantic] of type [semantic_text] can't be used in multifields"));
        } else {
            EndpointClusterState sparseModelSettings = createRandomModelSettings(TaskType.SPARSE_EMBEDDING);
            var mapperService = createSemanticMapperService(fieldMapping(b -> {
                b.field("type", "text");
                b.startObject("fields");
                addSemanticMapping(b, "semantic", "my_inference_id", null, sparseModelSettings, null, null);
                b.endObject();
            }));
            assertSemanticField(mapperService, "field.semantic", true, sparseModelSettings, null, null);

            mapperService = createSemanticMapperService(
                semanticMapping("field", "my_inference_id", null, sparseModelSettings, null, null, b -> {
                    b.startObject("fields");
                    b.startObject("text").field("type", "text").endObject();
                    b.endObject();
                })
            );
            assertSemanticField(mapperService, "field", true, sparseModelSettings, null, null);

            mapperService = createSemanticMapperService(
                semanticMapping("field", "my_inference_id", null, sparseModelSettings, null, null, b -> {
                    b.startObject("fields");
                    addSemanticMapping(b, "semantic", "another_inference_id", null, sparseModelSettings, null, null);
                    b.endObject();
                })
            );
            assertSemanticField(mapperService, "field", true, sparseModelSettings, null, null);
            assertSemanticField(mapperService, "field.semantic", true, sparseModelSettings, null, null);

            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createSemanticMapperService(semanticMapping("field", "my_inference_id", null, null, null, null, b -> {
                    b.startObject("fields");
                    b.startObject("inference").field("type", "text").endObject();
                    b.endObject();
                }))
            );
            assertThat(e.getMessage(), containsString("is already used by another field"));
        }
    }

    public void testDynamicUpdate() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";
        final String searchInferenceId = "search_test_service";
        final EndpointClusterState sparseModelSettings = createRandomModelSettings(TaskType.SPARSE_EMBEDDING);

        {
            MapperService mapperService = createSemanticMapperService(semanticMapping(fieldName, inferenceId));
            performDynamicUpdate(mapperService, fieldName, inferenceId, sparseModelSettings);
            assertSemanticField(mapperService, fieldName, true, sparseModelSettings, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }

        {
            MapperService mapperService = createSemanticMapperService(semanticMapping(fieldName, inferenceId, searchInferenceId));
            performDynamicUpdate(mapperService, fieldName, inferenceId, sparseModelSettings);
            assertSemanticField(mapperService, fieldName, true, sparseModelSettings, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId);
        }
    }

    public void testUpdateModelSettings() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String fieldName = randomFieldName(depth);
            EndpointClusterState sparseSettings = createRandomModelSettings(TaskType.SPARSE_EMBEDDING);
            EndpointClusterState denseSettings = createRandomModelSettings(TaskType.TEXT_EMBEDDING);
            MapperService mapperService = createSemanticMapperService(semanticMapping(fieldName, "test_model"));
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            {
                Exception exc = expectThrows(
                    MapperParsingException.class,
                    () -> merge(mapperService, semanticMapping(fieldName, "test_model", null, null, null, null, b -> {
                        b.startObject("model_settings");
                        b.field("inference_id", "test_model");
                        b.endObject();
                    }))
                );
                assertThat(exc.getMessage(), containsString("Required [task_type]"));
            }
            {
                merge(mapperService, semanticMapping(fieldName, "test_model", sparseSettings));
                assertSemanticField(mapperService, fieldName, true, sparseSettings, null, null);
            }
            {
                merge(mapperService, semanticMapping(fieldName, "test_model"));
                assertSemanticField(mapperService, fieldName, true, sparseSettings, null, null);
            }
            {
                Exception exc = expectThrows(
                    IllegalArgumentException.class,
                    () -> merge(mapperService, semanticMapping(fieldName, "test_model", denseSettings))
                );
                assertThat(exc.getMessage(), containsString("cannot be changed from type [sparse_vector] to [dense_vector]"));
            }
        }
    }

    public void testDenseVectorIndexOptionValidation() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String inferenceId = "test_model";
            String fieldName = randomFieldName(depth);

            DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions = DenseVectorFieldTypeTests.randomIndexOptionsAll();
            Exception exc = expectThrows(
                MapperParsingException.class,
                () -> createSemanticMapperService(semanticMapping(fieldName, inferenceId, null, null, null, null, b -> {
                    b.startObject(INDEX_OPTIONS_FIELD);
                    b.startObject("dense_vector");
                    b.field("type", indexOptions.getType().name().toLowerCase(Locale.ROOT));
                    b.field("unsupported_param", "any_value");
                    b.endObject();
                    b.endObject();
                }))
            );
            assertTrue(exc.getMessage().contains("unsupported parameters"));
        }
    }

    public void testSparseVectorIndexOptionsValidationAndMapping() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String inferenceId = "test_model";
            String fieldName = randomFieldName(depth);
            EndpointClusterState sparseSettings = createRandomModelSettings(TaskType.SPARSE_EMBEDDING);
            var sparseVectorIndexOptions = SparseVectorFieldTypeTests.randomSparseVectorIndexOptions();
            var expectedIndexOptions = sparseVectorIndexOptions == null
                ? null
                : new SemanticIndexOptions(SemanticIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, sparseVectorIndexOptions);

            // should not throw an exception
            MapperService mapper = createSemanticMapperService(
                semanticMapping(fieldName, inferenceId, null, sparseSettings, null, expectedIndexOptions)
            );

            assertSemanticField(mapper, fieldName, true, sparseSettings, null, expectedIndexOptions);
        }
    }

    public void testSparseVectorMappingUpdate() throws IOException {
        for (int i = 0; i < 5; i++) {
            Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
            when(globalModelRegistry.getEndpointClusterState(anyString())).thenAnswer(
                invocation -> { return new EndpointClusterState(model); }
            );

            final ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
            final SemanticIndexOptions indexOptions = randomSemanticIndexOptions(TaskType.SPARSE_EMBEDDING);
            final String fieldName = "field";

            MapperService mapperService = createSemanticMapperService(
                semanticMapping(fieldName, model.getInferenceEntityId(), null, null, chunkingSettings, indexOptions)
            );
            assertSemanticField(mapperService, fieldName, false, new EndpointClusterState(model), chunkingSettings, indexOptions);

            final SemanticIndexOptions newIndexOptions = randomSemanticIndexOptions(TaskType.SPARSE_EMBEDDING);
            final ChunkingSettings newChunkingSettings = generateRandomChunkingSettingsOtherThan(chunkingSettings);
            merge(
                mapperService,
                semanticMapping(fieldName, model.getInferenceEntityId(), null, null, newChunkingSettings, newIndexOptions)
            );
            assertSemanticField(mapperService, fieldName, false, new EndpointClusterState(model), newChunkingSettings, newIndexOptions);
        }
    }

    public void testUpdateSearchInferenceId() throws IOException {
        final String inferenceId = "test_inference_id";
        final String searchInferenceId1 = "test_search_inference_id_1";
        final String searchInferenceId2 = "test_search_inference_id_2";

        for (int depth = 1; depth < 5; depth++) {
            String fieldName = randomFieldName(depth);
            MapperService mapperService = createSemanticMapperService(semanticMapping(fieldName, inferenceId));
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            merge(mapperService, semanticMapping(fieldName, inferenceId, searchInferenceId1));
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId1);

            merge(mapperService, semanticMapping(fieldName, inferenceId, searchInferenceId2));
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId2);

            merge(mapperService, semanticMapping(fieldName, inferenceId));
            assertSemanticField(mapperService, fieldName, false, null, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            EndpointClusterState sparseSettings = createRandomModelSettings(TaskType.SPARSE_EMBEDDING);
            mapperService = createSemanticMapperService(semanticMapping(fieldName, inferenceId, sparseSettings));
            assertSemanticField(mapperService, fieldName, true, sparseSettings, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            merge(mapperService, semanticMapping(fieldName, inferenceId, searchInferenceId1));
            assertSemanticField(mapperService, fieldName, true, sparseSettings, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId1);

            merge(mapperService, semanticMapping(fieldName, inferenceId, searchInferenceId2));
            assertSemanticField(mapperService, fieldName, true, sparseSettings, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId2);

            merge(mapperService, semanticMapping(fieldName, inferenceId));
            assertSemanticField(mapperService, fieldName, true, sparseSettings, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }
    }

    @Override
    protected Class<SemanticTextFieldMapper> expectedMapperClass() {
        return SemanticTextFieldMapper.class;
    }

    @Override
    protected Class<SemanticTextFieldMapper.SemanticTextFieldType> expectedFieldTypeClass() {
        return SemanticTextFieldMapper.SemanticTextFieldType.class;
    }

    @Override
    protected String contentType() {
        return SemanticTextFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected Set<TaskType> supportedTaskTypes() {
        return EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING);
    }

    @Override
    protected IndexVersion getRandomCompatibleIndexVersion() {
        // TODO: Bias towards indexVersion.current()
        return SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat());
    }

    @Override
    protected void assertChunksTextField(SemanticTextFieldMapper.SemanticTextFieldType fieldType, NestedObjectMapper chunksMapper) {
        if (fieldType.useLegacyFormat()) {
            Mapper textMapper = chunksMapper.getMapper(TEXT_FIELD);
            assertNotNull(textMapper);
            assertThat(textMapper, instanceOf(KeywordFieldMapper.class));
            KeywordFieldMapper textFieldMapper = (KeywordFieldMapper) textMapper;
            assertThat(textFieldMapper.fieldType().indexType(), equalTo(IndexType.NONE));
        } else {
            super.assertChunksTextField(fieldType, chunksMapper);
        }
    }

    @Override
    protected void assertEmbeddingsField(
        MapperService mapperService,
        FieldMapper embeddingsMapper,
        EndpointClusterState modelSettings,
        @Nullable SemanticIndexOptions expectedIndexOptions
    ) {
        if (modelSettings.taskType() == TaskType.SPARSE_EMBEDDING) {
            assertThat(embeddingsMapper, instanceOf(SparseVectorFieldMapper.class));
            SparseVectorFieldMapper sparseVectorFieldMapper = (SparseVectorFieldMapper) embeddingsMapper;
            assertEquals(sparseVectorFieldMapper.fieldType().isStored(), useLegacyFormat() == false);

            IndexOptions expectedBaseIndexOptions = expectedIndexOptions == null ? null : expectedIndexOptions.indexOptions();
            assertEquals(expectedBaseIndexOptions, sparseVectorFieldMapper.fieldType().getIndexOptions());
        } else {
            super.assertEmbeddingsField(mapperService, embeddingsMapper, modelSettings, expectedIndexOptions);
        }
    }

    public void testSuccessfulParse() throws IOException {
        for (int depth = 1; depth < 4; depth++) {
            final IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);

            final String fieldName1 = randomFieldName(depth);
            final String fieldName2 = randomFieldName(depth + 1);
            final String searchInferenceId = randomAlphaOfLength(8);
            final boolean setSearchInferenceId = randomBoolean();

            TaskType taskType = TaskType.SPARSE_EMBEDDING;
            Model model1 = TestModel.createRandomInstance(taskType);
            Model model2 = TestModel.createRandomInstance(taskType);

            when(globalModelRegistry.getEndpointClusterState(anyString())).thenAnswer(invocation -> {
                var modelId = (String) invocation.getArguments()[0];
                if (modelId.equals(model1.getInferenceEntityId())) {
                    return new EndpointClusterState(model1);
                }
                if (modelId.equals(model2.getInferenceEntityId())) {
                    return new EndpointClusterState(model2);
                }
                return null;
            });

            ChunkingSettings chunkingSettings = null; // Some chunking settings configs can produce different Lucene docs counts
            SemanticIndexOptions indexOptions = randomSemanticIndexOptions(taskType);
            XContentBuilder mapping = mapping(b -> {
                addSemanticMapping(
                    b,
                    fieldName1,
                    model1.getInferenceEntityId(),
                    setSearchInferenceId ? searchInferenceId : null,
                    null,
                    chunkingSettings,
                    indexOptions
                );
                addSemanticMapping(
                    b,
                    fieldName2,
                    model2.getInferenceEntityId(),
                    setSearchInferenceId ? searchInferenceId : null,
                    null,
                    chunkingSettings,
                    indexOptions
                );
            });

            MapperService mapperService = createSemanticMapperServiceWithIndexVersion(mapping, indexVersion);
            assertSemanticField(mapperService, fieldName1, false, new EndpointClusterState(model1), null, indexOptions);
            assertInferenceEndpoints(
                mapperService,
                fieldName1,
                model1.getInferenceEntityId(),
                setSearchInferenceId ? searchInferenceId : model1.getInferenceEntityId()
            );
            assertSemanticField(mapperService, fieldName2, false, new EndpointClusterState(model2), null, indexOptions);
            assertInferenceEndpoints(
                mapperService,
                fieldName2,
                model2.getInferenceEntityId(),
                setSearchInferenceId ? searchInferenceId : model2.getInferenceEntityId()
            );

            DocumentMapper documentMapper = mapperService.documentMapper();
            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        useLegacyFormat,
                        b,
                        List.of(
                            randomSemanticText(
                                useLegacyFormat,
                                fieldName1,
                                model1,
                                chunkingSettings,
                                List.of("a b", "c"),
                                XContentType.JSON
                            ),
                            randomSemanticText(useLegacyFormat, fieldName2, model2, chunkingSettings, List.of("d e f"), XContentType.JSON)
                        )
                    )
                )
            );

            List<LuceneDocument> luceneDocs = doc.docs();
            assertEquals(4, luceneDocs.size());
            for (int i = 0; i < 3; i++) {
                assertEquals(doc.rootDoc(), luceneDocs.get(i).getParent());
            }
            // nested docs are in reversed order
            assertSparseFeatures(luceneDocs.get(0), getEmbeddingsFieldName(fieldName1), 2);
            assertSparseFeatures(luceneDocs.get(1), getEmbeddingsFieldName(fieldName1), 1);
            assertSparseFeatures(luceneDocs.get(2), getEmbeddingsFieldName(fieldName2), 3);
            assertEquals(doc.rootDoc(), luceneDocs.get(3));
            assertNull(luceneDocs.get(3).getParent());

            withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
                NestedDocuments nested = new NestedDocuments(
                    mapperService.mappingLookup(),
                    QueryBitSetProducer::new,
                    IndexVersion.current()
                );
                LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

                Set<SearchHit.NestedIdentity> visitedNestedIdentities = new HashSet<>();
                Set<SearchHit.NestedIdentity> expectedVisitedNestedIdentities = Set.of(
                    new SearchHit.NestedIdentity(getChunksFieldName(fieldName1), 0, null),
                    new SearchHit.NestedIdentity(getChunksFieldName(fieldName1), 1, null),
                    new SearchHit.NestedIdentity(getChunksFieldName(fieldName2), 0, null)
                );

                assertChildLeafNestedDocument(leaf, 0, 3, visitedNestedIdentities);
                assertChildLeafNestedDocument(leaf, 1, 3, visitedNestedIdentities);
                assertChildLeafNestedDocument(leaf, 2, 3, visitedNestedIdentities);
                assertEquals(expectedVisitedNestedIdentities, visitedNestedIdentities);

                assertNull(leaf.advance(3));
                assertEquals(3, leaf.doc());
                assertEquals(3, leaf.rootDoc());
                assertNull(leaf.nestedIdentity());

                IndexSearcher searcher = newSearcher(reader);
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value());
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a", "b")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value());
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName2, List.of("d")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value());
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName2, List.of("z")),
                        10
                    );
                    assertEquals(0, topDocs.totalHits.value());
                }
            });
        }
    }

    public void testMissingInferenceId() throws IOException {
        final MapperService mapperService = createSemanticMapperService(semanticMapping("field", "my_id"));

        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> mapperService.documentMapper()
                .parse(
                    semanticTextInferenceSource(
                        useLegacyFormat,
                        b -> b.startObject("field")
                            .startObject(INFERENCE_FIELD)
                            .field(MODEL_SETTINGS_FIELD, createRandomModelSettings(TaskType.SPARSE_EMBEDDING))
                            .field(CHUNKS_FIELD, useLegacyFormat ? List.of() : Map.of())
                            .endObject()
                            .endObject()
                    )
                )
        );
        assertThat(ex.getCause().getMessage(), containsString("Required [inference_id]"));
    }

    public void testMissingModelSettingsAndChunks() throws IOException {
        MapperService mapperService = createSemanticMapperService(semanticMapping("field", "my_id"));
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> mapperService.documentMapper()
                .parse(
                    semanticTextInferenceSource(
                        useLegacyFormat,
                        b -> b.startObject("field").startObject(INFERENCE_FIELD).field(INFERENCE_ID_FIELD, "my_id").endObject().endObject()
                    )
                )
        );
        // Model settings may be null here so we only error on chunks
        assertThat(ex.getCause().getMessage(), containsString("Required [chunks]"));
    }

    public void testMissingTaskType() throws IOException {
        MapperService mapperService = createSemanticMapperService(semanticMapping("field", "my_id"));
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> mapperService.documentMapper()
                .parse(
                    semanticTextInferenceSource(
                        useLegacyFormat,
                        b -> b.startObject("field")
                            .startObject(INFERENCE_FIELD)
                            .field(INFERENCE_ID_FIELD, "my_id")
                            .startObject(MODEL_SETTINGS_FIELD)
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                )
        );
        assertThat(ex.getCause().getMessage(), containsString("failed to parse field [model_settings]"));
    }

    public void testMultimodalChunksNotSupported() throws Exception {
        // Exclude dot product because the randomly generated embedding may not have unit length
        Model model = TestModel.createRandomInstance(TaskType.EMBEDDING, List.of(SimilarityMeasure.DOT_PRODUCT));
        MapperService mapperService = createSemanticMapperService(semanticMapping("field", model.getInferenceEntityId()));

        EmbeddingResults.Embedding<?> embedding = randomMultimodalEmbedding(model);
        SemanticTextField semanticTextField = new SemanticTextField(
            useLegacyFormat,
            "field",
            null,
            new SemanticTextField.InferenceResult(
                model.getInferenceEntityId(),
                new EndpointClusterState(model),
                null,
                Map.of("field", List.of(new SemanticTextField.Chunk(0, embedding.toBytesRef(XContentType.JSON.xContent()))))
            ),
            XContentType.JSON
        );

        String expectedMessage = useLegacyFormat
            ? "[chunks] text doesn't support values of type: VALUE_NULL"
            : "[semantic_text] field [field] does not support multimodal values";
        DocumentParsingException e = assertThrows(
            DocumentParsingException.class,
            () -> mapperService.documentMapper()
                .parse(semanticTextInferenceSource(useLegacyFormat, b -> b.field("field", semanticTextField)))
        );
        assertThat(rootCause(e).getMessage(), containsString(expectedMessage));
    }

    public void testDenseVectorElementType() throws IOException {
        final String fieldName = "field";
        final String inferenceId = "test_service";

        BiConsumer<MapperService, DenseVectorFieldMapper.ElementType> assertMapperService = (m, e) -> {
            SemanticTextFieldMapper semanticTextFieldMapper = getSemanticFieldMapper(m, fieldName);
            assertThat(semanticTextFieldMapper.fieldType().getModelSettings().elementType(), equalTo(e));
        };

        MapperService floatMapperService = createSemanticMapperService(
            semanticMapping(
                fieldName,
                inferenceId,
                new EndpointClusterState(
                    "my-service",
                    TaskType.TEXT_EMBEDDING,
                    1024,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );
        assertMapperService.accept(floatMapperService, DenseVectorFieldMapper.ElementType.FLOAT);

        MapperService byteMapperService = createSemanticMapperService(
            semanticMapping(
                fieldName,
                inferenceId,
                new EndpointClusterState(
                    "my-service",
                    TaskType.TEXT_EMBEDDING,
                    1024,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.BYTE
                )
            )
        );
        assertMapperService.accept(byteMapperService, DenseVectorFieldMapper.ElementType.BYTE);

        MapperService bitMapperService = createSemanticMapperService(
            semanticMapping(
                fieldName,
                inferenceId,
                new EndpointClusterState(
                    "my-service",
                    TaskType.TEXT_EMBEDDING,
                    1024,
                    SimilarityMeasure.L2_NORM,
                    DenseVectorFieldMapper.ElementType.BIT
                )
            )
        );
        assertMapperService.accept(bitMapperService, DenseVectorFieldMapper.ElementType.BIT);

        MapperService bfloat16MapperService = createSemanticMapperService(
            semanticMapping(
                fieldName,
                inferenceId,
                new EndpointClusterState(
                    "my-service",
                    TaskType.TEXT_EMBEDDING,
                    1024,
                    SimilarityMeasure.COSINE,
                    DenseVectorFieldMapper.ElementType.BFLOAT16
                )
            )
        );
        assertMapperService.accept(bfloat16MapperService, DenseVectorFieldMapper.ElementType.BFLOAT16);
    }

    public void testSettingAndUpdatingChunkingSettings() throws IOException {
        Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        when(globalModelRegistry.getEndpointClusterState(anyString())).thenAnswer(
            invocation -> { return new EndpointClusterState(model); }
        );

        final ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
        final SemanticIndexOptions indexOptions = randomSemanticIndexOptions(TaskType.SPARSE_EMBEDDING);
        String fieldName = "field";

        MapperService mapperService = createSemanticMapperService(
            semanticMapping(fieldName, model.getInferenceEntityId(), null, null, chunkingSettings, indexOptions)
        );
        assertSemanticField(mapperService, fieldName, false, new EndpointClusterState(model), chunkingSettings, indexOptions);

        ChunkingSettings newChunkingSettings = generateRandomChunkingSettingsOtherThan(chunkingSettings);
        merge(mapperService, semanticMapping(fieldName, model.getInferenceEntityId(), null, null, newChunkingSettings, indexOptions));
        assertSemanticField(mapperService, fieldName, false, new EndpointClusterState(model), newChunkingSettings, indexOptions);
    }

    public void testModelSettingsRequiredWithChunks() throws IOException {
        // Create inference results where model settings are set to null and chunks are provided
        TaskType taskType = TaskType.SPARSE_EMBEDDING;
        Model model = TestModel.createRandomInstance(taskType);

        when(globalModelRegistry.getEndpointClusterState(anyString())).thenAnswer(
            invocation -> { return new EndpointClusterState(model); }
        );

        ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
        SemanticIndexOptions indexOptions = randomSemanticIndexOptions(taskType);
        SemanticTextField randomSemanticText = randomSemanticText(
            useLegacyFormat,
            "field",
            model,
            chunkingSettings,
            List.of("a"),
            XContentType.JSON
        );
        SemanticTextField inferenceResults = new SemanticTextField(
            randomSemanticText.useLegacyFormat(),
            randomSemanticText.fieldName(),
            randomSemanticText.originalValues(),
            new SemanticTextField.InferenceResult(
                randomSemanticText.inference().inferenceId(),
                null,
                randomSemanticText.inference().chunkingSettings(),
                randomSemanticText.inference().chunks()
            ),
            randomSemanticText.contentType()
        );

        MapperService mapperService = createSemanticMapperService(
            semanticMapping("field", model.getInferenceEntityId(), null, null, chunkingSettings, indexOptions)
        );
        SourceToParse source = source(b -> addSemanticTextInferenceResults(useLegacyFormat, b, List.of(inferenceResults)));
        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            DocumentParsingException.class,
            () -> mapperService.documentMapper().parse(source)
        );
        assertThat(ex.getMessage(), containsString("[model_settings] must be set for field [field] when chunks are provided"));
    }

    public void testPre811IndexSemanticTextDenseVectorRaisesError() throws IOException {
        assumeTrue("Test only applies to legacy format", useLegacyFormat);
        Model model = TestModel.createRandomInstance(TaskType.TEXT_EMBEDDING);
        EndpointClusterState modelSettings = new EndpointClusterState(model);
        String fieldName = randomAlphaOfLength(8);

        MapperService mapperService = createSemanticMapperService(
            semanticMapping(fieldName, model.getInferenceEntityId()),
            IndexVersions.V_8_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.NEW_SPARSE_VECTOR)
        );
        assertSemanticField(mapperService, fieldName, false, null, null, null);

        merge(mapperService, semanticMapping(fieldName, model.getInferenceEntityId(), modelSettings));
        assertSemanticField(mapperService, fieldName, true, modelSettings, null, null);

        DocumentMapper documentMapper = mapperService.documentMapper();
        DocumentParsingException e = assertThrows(
            DocumentParsingException.class,
            () -> documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        true,
                        b,
                        List.of(randomSemanticText(true, fieldName, model, null, List.of("foo", "bar"), XContentType.JSON))
                    )
                )
            )
        );
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
        assertThat(e.getCause().getMessage(), equalTo(UNSUPPORTED_INDEX_MESSAGE));
    }

    public void testPre811IndexSemanticTextSparseVectorRaisesError() throws IOException {
        assumeTrue("Test only applies to legacy format", useLegacyFormat);
        Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        EndpointClusterState modelSettings = new EndpointClusterState(model);
        String fieldName = randomAlphaOfLength(8);

        MapperService mapperService = createSemanticMapperService(
            semanticMapping(fieldName, model.getInferenceEntityId()),
            IndexVersions.V_8_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.NEW_SPARSE_VECTOR)
        );
        assertSemanticField(mapperService, fieldName, false, null, null, null);

        merge(mapperService, semanticMapping(fieldName, model.getInferenceEntityId(), modelSettings));
        assertSemanticField(mapperService, fieldName, true, modelSettings, null, null);

        DocumentMapper documentMapper = mapperService.documentMapper();
        DocumentParsingException e = assertThrows(
            DocumentParsingException.class,
            () -> documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        true,
                        b,
                        List.of(randomSemanticText(true, fieldName, model, null, List.of("foo", "bar"), XContentType.JSON))
                    )
                )
            )
        );
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
        assertThat(e.getCause().getMessage(), equalTo(UNSUPPORTED_INDEX_MESSAGE));
    }

    public void testExistsQuerySparseVector() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";

        MapperService mapperService = createSemanticMapperService(
            semanticMapping(fieldName, inferenceId, createRandomModelSettings(TaskType.SPARSE_EMBEDDING))
        );

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
        Query existsQuery = getSemanticFieldMapper(mapperService, fieldName).fieldType().existsQuery(searchExecutionContext);
        assertThat(existsQuery, instanceOf(ESToParentBlockJoinQuery.class));
    }

    public void testExistsQueryDenseVector() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";

        MapperService mapperService = createSemanticMapperService(
            semanticMapping(fieldName, inferenceId, createRandomModelSettings(TaskType.TEXT_EMBEDDING))
        );

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
        Query existsQuery = getSemanticFieldMapper(mapperService, fieldName).fieldType().existsQuery(searchExecutionContext);
        assertThat(existsQuery, instanceOf(ESToParentBlockJoinQuery.class));
    }

    public void testDynamicUpdateDiscardsEndpointMetadata() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";
        final EndpointMetadata endpointMetadata = EndpointMetadataTests.randomNonEmptyInstance();
        final EndpointClusterState modelSettings = new EndpointClusterState(
            "test-service",
            TaskType.SPARSE_EMBEDDING,
            null,
            null,
            null,
            EndpointMetadataClusterState.from(endpointMetadata)
        );

        // The dynamic update path discards endpoint metadata; only the core model settings are persisted in the mapping
        MapperService mapperService = createSemanticMapperService(semanticMapping(fieldName, inferenceId));
        performDynamicUpdate(mapperService, fieldName, inferenceId, modelSettings);
        SemanticTextFieldMapper mapper = getSemanticFieldMapper(mapperService, fieldName);
        assertThat(mapper.fieldType().getModelSettings().endpointMetadata(), equalTo(EndpointMetadataClusterState.EMPTY_INSTANCE));
    }

    public void testMappingWithEndpointMetadata() throws IOException {
        final EndpointMetadata endpointMetadata = EndpointMetadataTests.randomNonEmptyInstance();
        final EndpointMetadataClusterState endpointMetadataClusterState = EndpointMetadataClusterState.from(endpointMetadata);
        final EndpointClusterState modelSettingsWithMetadata = new EndpointClusterState(
            "test-service",
            TaskType.SPARSE_EMBEDDING,
            null,
            null,
            null,
            endpointMetadataClusterState
        );

        MapperService originalMapperService = createSemanticMapperService(
            semanticMapping("field", "test_service", null, null, null, null, b -> {
                b.field("model_settings", modelSettingsWithMetadata);
            })
        );
        SemanticTextFieldMapper originalMapper = getSemanticFieldMapper(originalMapperService, "field");
        assertThat(originalMapper.fieldType().getModelSettings().endpointMetadata(), equalTo(endpointMetadataClusterState));

        // An XContent serialization cycle should remove the endpoint metadata
        CompressedXContent mappingSource = originalMapperService.documentMapper().mappingSource();
        MapperService parsedMapperService = createSemanticMapperService(mapping(b -> {}));
        parsedMapperService.merge("_doc", mappingSource, MapperService.MergeReason.MAPPING_UPDATE);

        SemanticTextFieldMapper parsedMapper = getSemanticFieldMapper(parsedMapperService, "field");
        assertThat(parsedMapper.fieldType().getModelSettings().endpointMetadata(), equalTo(EndpointMetadataClusterState.EMPTY_INSTANCE));
    }

    private static DenseVectorFieldMapper.DenseVectorIndexOptions defaultBbqHnswDenseVectorIndexOptions() {
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        DenseVectorFieldMapper.RescoreVector rescoreVector = new DenseVectorFieldMapper.RescoreVector(DEFAULT_RESCORE_OVERSAMPLE);
        return new DenseVectorFieldMapper.BBQHnswIndexOptions(m, efConstruction, false, rescoreVector, -1);
    }

    public void testDefaultIndexOptions() throws IOException {
        for (int i = 0; i < 200; i++) {
            final EndpointClusterState modelSettings = createRandomModelSettings();
            final IndexVersion indexVersion = useLegacyFormat == false && randomBoolean()
                ? IndexVersion.current()
                : SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);

            MapperService mapperService = createSemanticMapperServiceWithIndexVersion(
                semanticMapping("field", "another_inference_id", modelSettings),
                indexVersion
            );
            assertSemanticField(mapperService, "field", true, modelSettings, null, null);
        }
    }

    public void testSpecifiedDenseVectorIndexOptions() throws IOException {
        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat());
        EndpointClusterState denseModelSettings = new EndpointClusterState(
            randomAlphaOfLength(4),
            TaskType.TEXT_EMBEDDING,
            100,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );

        // Specifying index options will override default index option settings
        SemanticIndexOptions expectedIndexOptions = new SemanticIndexOptions(
            SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
            new ExtendedDenseVectorIndexOptions(
                new DenseVectorFieldMapper.Int4HnswIndexOptions(20, 90, false, null, -1),
                DenseVectorFieldMapper.ElementType.FLOAT  // Explicitly override element type to hold it constant
            )
        );
        var mapperService = createSemanticMapperServiceWithIndexVersion(
            semanticMapping("field", "another_inference_id", null, denseModelSettings, null, expectedIndexOptions),
            indexVersion
        );
        assertSemanticField(mapperService, "field", true, denseModelSettings, null, expectedIndexOptions);

        // Specifying partial index options will fill in the remainder index options with defaults
        mapperService = createSemanticMapperServiceWithIndexVersion(
            semanticMapping("field", "another_inference_id", null, denseModelSettings, null, null, b -> {
                b.startObject("index_options");
                b.startObject("dense_vector");
                b.field("element_type", "float");  // Explicitly override element type to hold it constant
                b.field("type", "int4_hnsw");
                b.endObject();
                b.endObject();
            }),
            indexVersion
        );
        assertSemanticField(
            mapperService,
            "field",
            true,
            denseModelSettings,
            null,
            new SemanticIndexOptions(
                SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                new ExtendedDenseVectorIndexOptions(
                    new DenseVectorFieldMapper.Int4HnswIndexOptions(16, 100, false, null, -1),
                    DenseVectorFieldMapper.ElementType.FLOAT
                )
            )
        );

        // Incompatible index options will fail
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(
                semanticMapping(
                    "field",
                    "another_inference_id",
                    null,
                    createRandomModelSettings(TaskType.SPARSE_EMBEDDING),
                    null,
                    null,
                    b -> {
                        b.startObject("index_options");
                        b.startObject("dense_vector");
                        b.field("type", "int8_hnsw");
                        b.endObject();
                        b.endObject();
                    }
                ),
                indexVersion
            )
        );
        assertThat(e.getMessage(), containsString("Invalid task type"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(
                semanticMapping("field", "another_inference_id", null, denseModelSettings, null, null, b -> {
                    b.startObject("index_options");
                    b.startObject("dense_vector");
                    b.field("type", "bbq_flat");
                    b.field("ef_construction", 100);
                    b.endObject();
                    b.endObject();
                }),
                indexVersion
            )
        );
        assertThat(e.getMessage(), containsString("unsupported parameters:  [ef_construction : 100]"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(
                semanticMapping("field", "another_inference_id", null, denseModelSettings, null, null, b -> {
                    b.startObject("index_options");
                    b.startObject("dense_vector");
                    b.field("type", "invalid");
                    b.endObject();
                    b.endObject();
                }),
                indexVersion
            )
        );
        assertThat(e.getMessage(), containsString("Unsupported index options type invalid"));
    }

    public void testSetElementTypeInDenseVectorIndexOptions() throws IOException {
        EndpointClusterState denseModelSettings = new EndpointClusterState(
            randomAlphaOfLength(4),
            TaskType.TEXT_EMBEDDING,
            100,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );

        // Specifying the element type prevents defaulting to bfloat16
        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);
        var mapperService = createSemanticMapperServiceWithIndexVersion(
            semanticMapping(
                "field",
                "another_inference_id",
                null,
                denseModelSettings,
                null,
                new SemanticIndexOptions(
                    SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                    new ExtendedDenseVectorIndexOptions(null, DenseVectorFieldMapper.ElementType.FLOAT)
                )
            ),
            indexVersion
        );

        SemanticIndexOptions expectedDefaultIndexOptions = getDefaultIndexOptions(denseModelSettings, mapperService);
        DenseVectorFieldMapper.DenseVectorIndexOptions expectedDenseVectorIndexOptions = expectedDefaultIndexOptions != null
            ? ((ExtendedDenseVectorIndexOptions) expectedDefaultIndexOptions.indexOptions()).getBaseIndexOptions()
            : null;

        assertSemanticField(
            mapperService,
            "field",
            true,
            denseModelSettings,
            null,
            new SemanticIndexOptions(
                SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                new ExtendedDenseVectorIndexOptions(expectedDenseVectorIndexOptions, DenseVectorFieldMapper.ElementType.FLOAT)
            )
        );

        // Can use element type in combination with type
        SemanticIndexOptions int4HnswWithFloatIndexOptions = new SemanticIndexOptions(
            SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
            new ExtendedDenseVectorIndexOptions(
                new DenseVectorFieldMapper.Int4HnswIndexOptions(20, 90, false, null, -1),
                DenseVectorFieldMapper.ElementType.FLOAT
            )
        );
        mapperService = createSemanticMapperServiceWithIndexVersion(
            semanticMapping("field", "another_inference_id", null, denseModelSettings, null, int4HnswWithFloatIndexOptions),
            indexVersion
        );

        assertSemanticField(mapperService, "field", true, denseModelSettings, null, int4HnswWithFloatIndexOptions);
    }

    public void testInvalidElementTypeOverride() {
        for (int i = 0; i < 10; i++) {
            final EndpointClusterState modelSettings = createRandomModelSettings(TaskType.TEXT_EMBEDDING);
            final DenseVectorFieldMapper.ElementType modelElementType = modelSettings.elementType();

            final DenseVectorFieldMapper.ElementType overrideElementType;
            if (modelElementType == DenseVectorFieldMapper.ElementType.FLOAT) {
                overrideElementType = randomFrom(DenseVectorFieldMapper.ElementType.BYTE, DenseVectorFieldMapper.ElementType.BIT);
            } else {
                overrideElementType = randomValueOtherThan(modelElementType, () -> randomFrom(DenseVectorFieldMapper.ElementType.values()));
            }

            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createSemanticMapperService(
                    semanticMapping(
                        "field",
                        "test_inference_id",
                        null,
                        modelSettings,
                        null,
                        new SemanticIndexOptions(
                            SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                            new ExtendedDenseVectorIndexOptions(null, overrideElementType)
                        )
                    )
                )
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "Model element type [" + modelElementType + "] is incompatible with element type override [" + overrideElementType + "]"
                )
            );
        }
    }

    public void testSpecificSparseVectorIndexOptions() throws IOException {
        for (int i = 0; i < 10; i++) {
            IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat());
            SparseVectorFieldMapper.SparseVectorIndexOptions testIndexOptions = randomSparseVectorIndexOptions(false);
            EndpointClusterState sparseModelSettings = createRandomModelSettings(TaskType.SPARSE_EMBEDDING);
            var mapperService = createSemanticMapperServiceWithIndexVersion(
                semanticMapping(
                    "field",
                    "test_inference_id",
                    null,
                    sparseModelSettings,
                    null,
                    new SemanticIndexOptions(SemanticIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, testIndexOptions)
                ),
                indexVersion
            );

            assertSemanticField(
                mapperService,
                "field",
                true,
                sparseModelSettings,
                null,
                new SemanticIndexOptions(SemanticIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, testIndexOptions)
            );
        }
    }

    public void testSparseVectorIndexOptionsValidations() throws IOException {
        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat());
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(semanticMapping("field", "test_inference_id", null, null, null, null, b -> {
                b.startObject(INDEX_OPTIONS_FIELD);
                b.startObject(SparseVectorFieldMapper.CONTENT_TYPE);
                b.field("prune", false);
                b.startObject("pruning_config");
                b.field(TokenPruningConfig.TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName(), 5.0f);
                b.endObject();
                b.endObject();
                b.endObject();
            }), indexVersion)
        );
        assertThat(e.getMessage(), containsString("failed to parse field [pruning_config]"));

        e = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(semanticMapping("field", "test_inference_id", null, null, null, null, b -> {
                b.startObject(INDEX_OPTIONS_FIELD);
                b.startObject(SparseVectorFieldMapper.CONTENT_TYPE);
                b.field("prune", true);
                b.startObject("pruning_config");
                b.field(TokenPruningConfig.TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName(), 1000.0f);
                b.endObject();
                b.endObject();
                b.endObject();
            }), indexVersion)
        );
        var innerClause = e.getCause().getCause().getCause();
        assertThat(innerClause.getMessage(), containsString("[tokens_freq_ratio_threshold] must be between [1] and [100], got 1000.0"));
    }

    @Override
    public void testSupportedIndexVersions() throws IOException {
        Model sparseModel = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        Model denseModel = TestModel.createRandomInstance(TaskType.TEXT_EMBEDDING);

        Set<IndexVersion> supportedVersions = getSupportedVersions();

        for (int i = 0; i < Math.min(supportedVersions.size(), 100); i++) {
            IndexVersion indexVersion = randomFrom(supportedVersions);
            createMapperService(
                indexVersion,
                semanticMapping("field", denseModel.getInferenceEntityId(), new EndpointClusterState(denseModel))
            );
            createMapperService(
                indexVersion,
                semanticMapping("field", sparseModel.getInferenceEntityId(), new EndpointClusterState(sparseModel))
            );
        }
    }

    @Override
    protected MapperService createSemanticMapperService(XContentBuilder mappings, IndexVersion minIndexVersion) throws IOException {
        IndexVersion maxIndexVersion = useLegacyFormat()
            ? IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN)
            : IndexVersion.current();
        return createSemanticMapperService(mappings, minIndexVersion, maxIndexVersion);
    }

    @Override
    protected MapperService createSemanticMapperServiceWithIndexVersion(XContentBuilder mappings, IndexVersion indexVersion)
        throws IOException {
        validateIndexVersion(indexVersion, useLegacyFormat());
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat())
            .build();
        return createMapperService(indexVersion, settings, mappings);
    }

    public static SemanticIndexOptions randomSemanticIndexOptions() {
        TaskType taskType = randomFrom(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING);
        return randomSemanticIndexOptions(taskType);
    }

    public static SemanticIndexOptions randomSemanticIndexOptions(TaskType taskType) {
        if (taskType == TaskType.TEXT_EMBEDDING) {
            return randomBoolean()
                ? null
                : new SemanticIndexOptions(SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR, randomIndexOptionsAll());
        }

        if (taskType == TaskType.SPARSE_EMBEDDING) {
            return randomBoolean()
                ? null
                : new SemanticIndexOptions(SemanticIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, randomSparseVectorIndexOptions(false));
        }

        return null;
    }

    public static void addSemanticTextInferenceResults(
        boolean useLegacyFormat,
        XContentBuilder sourceBuilder,
        List<SemanticTextField> semanticTextInferenceResults
    ) throws IOException {
        if (useLegacyFormat) {
            for (var field : semanticTextInferenceResults) {
                sourceBuilder.field(field.fieldName());
                sourceBuilder.value(field);
            }
        } else {
            // Use a linked hash map to maintain insertion-order iteration over the inference fields
            Map<String, Object> inferenceMetadataFields = new LinkedHashMap<>();
            for (var field : semanticTextInferenceResults) {
                inferenceMetadataFields.put(field.fieldName(), field);
            }
            sourceBuilder.field(InferenceMetadataFieldsMapper.NAME, inferenceMetadataFields);
        }
    }

    private static Query generateNestedTermSparseVectorQuery(NestedLookup nestedLookup, String fieldName, List<String> tokens) {
        NestedObjectMapper mapper = nestedLookup.getNestedMappers().get(getChunksFieldName(fieldName));
        assertNotNull(mapper);

        BitSetProducer parentFilter = new QueryBitSetProducer(Queries.newNonNestedFilter(IndexVersion.current()));
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (String token : tokens) {
            queryBuilder.add(
                new BooleanClause(new TermQuery(new Term(getEmbeddingsFieldName(fieldName), token)), BooleanClause.Occur.MUST)
            );
        }
        queryBuilder.add(new BooleanClause(mapper.nestedTypeFilter(), BooleanClause.Occur.FILTER));

        return new ESToParentBlockJoinQuery(
            new SparseVectorQueryWrapper(fieldName, queryBuilder.build()),
            parentFilter,
            ScoreMode.Total,
            null
        );
    }

    private SourceToParse semanticTextInferenceSource(boolean useLegacyFormat, CheckedConsumer<XContentBuilder, IOException> build)
        throws IOException {
        return source(b -> {
            if (useLegacyFormat == false) {
                b.startObject(InferenceMetadataFieldsMapper.NAME);
            }
            build.accept(b);
            if (useLegacyFormat == false) {
                b.endObject();
            }
        });
    }

    private static void assertChildLeafNestedDocument(
        LeafNestedDocuments leaf,
        int advanceToDoc,
        int expectedRootDoc,
        Set<SearchHit.NestedIdentity> visitedNestedIdentities
    ) throws IOException {

        assertNotNull(leaf.advance(advanceToDoc));
        assertEquals(advanceToDoc, leaf.doc());
        assertEquals(expectedRootDoc, leaf.rootDoc());
        assertNotNull(leaf.nestedIdentity());
        visitedNestedIdentities.add(leaf.nestedIdentity());
    }

    private static void assertSparseFeatures(LuceneDocument doc, String fieldName, int expectedCount) {
        int count = 0;
        for (IndexableField field : doc.getFields()) {
            if (field instanceof FeatureField featureField) {
                assertThat(featureField.name(), equalTo(fieldName));
                ++count;
            }
        }
        assertThat(count, equalTo(expectedCount));
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static void validateIndexVersion(IndexVersion indexVersion, boolean useLegacyFormat) {
        if (useLegacyFormat == false
            && indexVersion.before(IndexVersions.INFERENCE_METADATA_FIELDS)
            && indexVersion.between(IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT, IndexVersions.UPGRADE_TO_LUCENE_10_0_0) == false) {
            throw new AssertionError("Index version " + indexVersion + " does not support new semantic text format");
        }

        if (useLegacyFormat && indexVersion.onOrAfter(IndexVersions.SEMANTIC_TEXT_LEGACY_FORMAT_FORBIDDEN)) {
            throw new AssertionError("Index version " + indexVersion + " does not support legacy semantic text format");
        }
    }
}
