/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.License;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class SemanticFieldMapperTests extends AbstractSemanticMapperTestCase<SemanticFieldMapper, SemanticFieldMapper.SemanticFieldType> {
    private static final String INFERENCE_ID = "inference-id";

    public SemanticFieldMapperTests(License.OperationMode operationMode) {
        super(operationMode);
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

    private void minimalMappingWithModelSettings(XContentBuilder b) throws IOException {
        MinimalServiceSettings modelSettings = new MinimalServiceSettings(
            "test_service",
            TaskType.EMBEDDING,
            128,
            SimilarityMeasure.COSINE,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
        addSemanticMapping(b, "my_field", "test_model", null, modelSettings, null, null);
    }

    /**
     * In synthetic-source (and columnar) indices, a {@code semantic} field rebuilds {@code _source} from its internal binary doc
     * values store. Text round-trips as a string; an image (base64 data URI) round-trips as a {@code {type, format, value}} object,
     * with the base64 payload stored decoded and regenerated on read.
     */
    public void testOriginalValueRoundTripFromDocValues() throws IOException {
        MapperService mapperService = createSemanticMapperServiceWithSourceMode(
            mapping(this::minimalMappingWithModelSettings),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        DocumentMapper mapper = mapperService.documentMapper();

        assertThat(syntheticSource(mapper, b -> b.field("my_field", "hello")), equalTo("{\"my_field\":\"hello\"}"));

        String dataUri = dataUri(new byte[] { 1, 2, 3, 4, 5 });
        assertThat(
            syntheticSource(
                mapper,
                b -> b.startObject("my_field").field("type", "image").field("format", "base64").field("value", dataUri).endObject()
            ),
            equalTo("{\"my_field\":" + imageObject(dataUri) + "}")
        );

        // A mixed text/image array preserves document order and types.
        assertThat(syntheticSource(mapper, b -> {
            b.startArray("my_field");
            b.value("first");
            b.startObject().field("type", "image").field("format", "base64").field("value", dataUri).endObject();
            b.value("last");
            b.endArray();
        }), equalTo("{\"my_field\":[\"first\"," + imageObject(dataUri) + ",\"last\"]}"));
    }

    /**
     * Columnar index modes reject any field whose {@code _source} is not reconstructable from doc values. A {@code semantic} field is
     * accepted because its original value is stored in doc values and its internal inference sub-fields are exempt.
     */
    public void testSemanticFieldAcceptedInColumnar() throws IOException {
        String dvFieldName = SemanticTextField.getOriginalValuesFieldName("my_field");
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            // Mapping creation succeeding is the assertion: the columnar "every field reconstructable from doc values" check passes.
            MapperService mapperService = createSemanticMapperServiceWithIndexMode(
                mapping(this::minimalMappingWithModelSettings),
                IndexVersion.current(),
                indexMode
            );
            ParsedDocument doc = mapperService.documentMapper()
                .parse(source(b -> b.field("@timestamp", "2024-01-01T00:00:00Z").field("my_field", "hello")));
            assertNotNull("original value stored in doc values", doc.rootDoc().getField(dvFieldName));

            // An object (image) value must reach the field's parser under columnar's subobjects:false, not be flattened into dotted
            // sub-fields (which would drop it from the doc values store).
            ParsedDocument imageDoc = mapperService.documentMapper().parse(source(b -> {
                b.field("@timestamp", "2024-01-01T00:00:00Z");
                b.startObject("my_field").field("type", "image").field("value", dataUri(new byte[] { 1, 2, 3 })).endObject();
            }));
            assertNotNull("image value stored in doc values", imageDoc.rootDoc().getField(dvFieldName));
        }
    }

    /**
     * When {@code _source} is rebuilt from doc values, the {@code fields} option (and highlighting) reads the original value straight
     * from the binary doc values store: text comes back as a string and a data URI as its {@code {type, format, value}} object.
     */
    public void testOriginalValueFetchedFromDocValues() throws IOException {
        MapperService mapperService = createSemanticMapperServiceWithSourceMode(
            mapping(this::minimalMappingWithModelSettings),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        // A random mix of text, boolean, numeric and multimodal (InferenceString) values, to cover all decoded forms.
        List<Object> inputs = randomList(1, 5, () -> SemanticTextFieldTests.randomSemanticInput(true));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startArray("my_field");
            for (Object input : inputs) {
                writeSemanticInput(b, input);
            }
            b.endArray();
        }));

        SearchExecutionContext searchContext = createSearchExecutionContext(mapperService);
        ValueFetcher fetcher = searchContext.getFieldType("my_field").valueFetcher(searchContext, null);
        assertThat(fetcher, instanceOf(OriginalValuesDocValuesFetcher.class));
        assertThat(fetcher.storedFieldsSpec(), equalTo(StoredFieldsSpec.NO_REQUIREMENTS));

        List<Object> expected = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            expected.add(expectedFetchedValue(input));
        }

        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            fetcher.setNextReader(reader.leaves().get(0));
            // An empty Source proves the values come from doc values, in document order.
            List<Object> values = fetcher.fetchValues(Source.empty(XContentType.JSON), 0, new ArrayList<>());
            assertThat(values, equalTo(expected));
        });
    }

    private static void writeSemanticInput(XContentBuilder b, Object input) throws IOException {
        if (input instanceof InferenceString inferenceString) {
            inferenceString.toXContent(b, ToXContent.EMPTY_PARAMS);
        } else {
            b.value(input);
        }
    }

    /** The form the doc-values value fetcher returns: a {type, format, value} map for a data URI, otherwise the value's text form. */
    private Object expectedFetchedValue(Object input) throws IOException {
        if (input instanceof InferenceString inferenceString) {
            // The encoder stores the decoded bytes and regenerates canonical (padded) base64 on read, so a non-canonical input
            // payload comes back normalized. Mirror that by decoding and re-encoding the payload here.
            String dataUri = inferenceString.value();
            int dataStart = dataUri.indexOf(',') + 1;
            String canonicalDataUri = dataUri.substring(0, dataStart) + Base64.getEncoder()
                .encodeToString(Base64.getDecoder().decode(dataUri.substring(dataStart)));
            return Map.of(
                InferenceString.TYPE_FIELD,
                inferenceString.dataType().toString(),
                InferenceString.FORMAT_FIELD,
                inferenceString.dataType().getDefaultFormat().toString(),
                InferenceString.VALUE_FIELD,
                canonicalDataUri
            );
        }
        // Booleans and numbers are stored via parser.text(); assert against Object.toString() so this also verifies the two agree
        // (the value fetcher reading from _source uses toString()).
        return input.toString();
    }

    /**
     * A {@code semantic} value may be a boolean or a number; like the inference filter (SemanticTextUtils#nodeStringValues), the field
     * coerces it to its string form, so the doc-values {@code _source} round-trip returns the value as a string.
     */
    public void testBooleanAndNumericValuesRoundTripAsStrings() throws IOException {
        MapperService mapperService = createSemanticMapperServiceWithSourceMode(
            mapping(this::minimalMappingWithModelSettings),
            IndexVersion.current(),
            SourceFieldMapper.Mode.SYNTHETIC
        );
        DocumentMapper mapper = mapperService.documentMapper();

        assertThat(syntheticSource(mapper, b -> b.field("my_field", true)), equalTo("{\"my_field\":\"true\"}"));
        assertThat(syntheticSource(mapper, b -> b.field("my_field", 42)), equalTo("{\"my_field\":\"42\"}"));
        assertThat(syntheticSource(mapper, b -> b.field("my_field", 1.5)), equalTo("{\"my_field\":\"1.5\"}"));

        // A mixed array of a string, boolean and number preserves document order, with each non-string value coerced to a string.
        assertThat(syntheticSource(mapper, b -> {
            b.startArray("my_field");
            b.value("text");
            b.value(false);
            b.value(7);
            b.endArray();
        }), equalTo("{\"my_field\":[\"text\",\"false\",\"7\"]}"));
    }

    /**
     * The semantic highlighter can skip {@code _source} when the field's value is retrievable from doc values (synthetic source),
     * but must still load {@code _source} when the value is kept there (stored source).
     */
    public void testHighlighterAvoidsSourceWhenValuesInDocValues() throws IOException {
        SemanticTextHighlighter highlighter = new SemanticTextHighlighter();
        IndexVersion version = IndexVersion.current();

        SearchExecutionContext syntheticContext = createSearchExecutionContext(
            createSemanticMapperServiceWithSourceMode(
                mapping(this::minimalMappingWithModelSettings),
                version,
                SourceFieldMapper.Mode.SYNTHETIC
            )
        );
        assertTrue(highlighter.canHighlightWithoutSource(syntheticContext.getFieldType("my_field"), syntheticContext));

        SearchExecutionContext storedContext = createSearchExecutionContext(
            createSemanticMapperServiceWithSourceMode(
                mapping(this::minimalMappingWithModelSettings),
                version,
                SourceFieldMapper.Mode.STORED
            )
        );
        assertFalse(highlighter.canHighlightWithoutSource(storedContext.getFieldType("my_field"), storedContext));
    }

    private static String dataUri(byte[] payload) {
        return "data:image/png;base64," + Base64.getEncoder().encodeToString(payload);
    }

    private static String imageObject(String dataUri) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder();
        new InferenceString(DataType.IMAGE, dataUri).toXContent(b, ToXContent.EMPTY_PARAMS);
        return Strings.toString(b);
    }

    public void testSemanticFieldNotSupportedOnOldIndices() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.SEMANTIC_FIELD_TYPE);

        var ex = expectThrows(
            MapperParsingException.class,
            () -> createSemanticMapperServiceWithIndexVersion(semanticMapping("my_field", "test_model"), oldVersion)
        );
        assertThat(ex.getMessage(), containsString("[" + SemanticFieldMapper.CONTENT_TYPE + "]"));
        assertThat(ex.getMessage(), containsString("is not supported on indices created before version"));
        assertThat(ex.getMessage(), containsString(IndexVersions.SEMANTIC_FIELD_TYPE.toString()));
    }

    public void testSemanticFieldSupportedOnNewIndices() throws IOException {
        IndexVersion newVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE);

        // Should not throw; model_settings provided to avoid consulting the model registry
        var mapperService = createSemanticMapperServiceWithIndexVersion(mapping(this::minimalMappingWithModelSettings), newVersion);
        assertNotNull(mapperService);
        assertSemanticFieldMapper(mapperService, "my_field");
    }

    public void testSemanticFieldMappingUpdateNotSupportedOnOldIndices() throws IOException {
        IndexVersion oldVersion = IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.SEMANTIC_FIELD_TYPE);

        var mapperService = createSemanticMapperServiceWithIndexVersion(mapping(b -> {}), oldVersion);

        var ex = expectThrows(MapperParsingException.class, () -> merge(mapperService, semanticMapping("my_field", "test_model")));
        assertThat(ex.getMessage(), containsString("[" + SemanticFieldMapper.CONTENT_TYPE + "]"));
        assertThat(ex.getMessage(), containsString("is not supported on indices created before version"));
        assertThat(ex.getMessage(), containsString(IndexVersions.SEMANTIC_FIELD_TYPE.toString()));
    }

    public void testSemanticFieldMappingUpdateSupportedOnNewIndices() throws IOException {
        IndexVersion newVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE);

        var mapperService = createSemanticMapperServiceWithIndexVersion(mapping(b -> {}), newVersion);
        assertNotNull(mapperService);
        // Should not throw; model_settings provided to avoid consulting the model registry
        merge(mapperService, mapping(this::minimalMappingWithModelSettings));

        assertSemanticFieldMapper(mapperService, "my_field");
    }

    private static void assertSemanticFieldMapper(MapperService mapperService, String fieldName) {
        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertThat(mapper, instanceOf(SemanticFieldMapper.class));
    }

    @Override
    protected Class<SemanticFieldMapper> expectedMapperClass() {
        return SemanticFieldMapper.class;
    }

    @Override
    protected Class<SemanticFieldMapper.SemanticFieldType> expectedFieldTypeClass() {
        return SemanticFieldMapper.SemanticFieldType.class;
    }

    @Override
    protected String contentType() {
        return SemanticFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected Set<TaskType> supportedTaskTypes() {
        return EnumSet.of(EMBEDDING);
    }

    @Override
    protected IndexVersion getRandomCompatibleIndexVersion() {
        return IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.SEMANTIC_FIELD_TYPE);
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
