/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexVersions.NEW_SPARSE_VECTOR;
import static org.elasticsearch.index.IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT;
import static org.elasticsearch.index.IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT_BACKPORT_8_X;
import static org.elasticsearch.index.IndexVersions.UPGRADE_TO_LUCENE_10_0_0;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SparseVectorFieldMapperTests extends SyntheticVectorsMapperTestCase {

    public static final float STRICT_TOKENS_WEIGHT_THRESHOLD = 0.5f;
    public static final float STRICT_TOKENS_FREQ_RATIO_THRESHOLD = 1;

    private static final Map<String, Float> COMMON_TOKENS = Map.of(
        "common1_drop_default",
        0.1f,
        "common2_drop_default",
        0.1f,
        "common3_drop_default",
        0.1f
    );

    private static final Map<String, Float> MEDIUM_TOKENS = Map.of("medium1_keep_strict", 0.5f, "medium2_keep_default", 0.25f);

    private static final Map<String, Float> RARE_TOKENS = Map.of("rare1_keep_strict", 0.9f, "rare2_keep_strict", 0.85f);

    @Override
    protected Object getSampleValueForDocument() {
        return new TreeMap<>(
            randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLengthBetween(5, 10), Float.valueOf(randomIntBetween(1, 127))))
        );
    }

    @Override
    protected Object getSampleValueForDocument(boolean binaryFormat) {
        return getSampleValueForDocument();
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
    }

    @Override
    protected boolean supportsEmptyInputArray() {
        return false;
    }

    @Override
    protected boolean addsValueWhenNotSupplied() {
        return true;
    }

    protected void minimalFieldMappingPreviousIndexDefaultsIncluded(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.field("store", false);

        b.startObject("meta");
        b.endObject();

        b.field("index_options", (Object) null);
    }

    protected void minimalMappingWithExplicitDefaults(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.field("store", true);

        b.startObject("meta");
        b.endObject();

        b.startObject("index_options");
        {
            b.field("prune", true);
            b.startObject("pruning_config");
            {
                b.field("tokens_freq_ratio_threshold", TokenPruningConfig.DEFAULT_TOKENS_FREQ_RATIO_THRESHOLD);
                b.field("tokens_weight_threshold", TokenPruningConfig.DEFAULT_TOKENS_WEIGHT_THRESHOLD);
            }
            b.endObject();
        }
        b.endObject();
    }

    protected void minimalMappingWithExplicitIndexOptions(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.startObject("index_options");
        {
            b.field("prune", true);
            b.startObject("pruning_config");
            {
                b.field("tokens_freq_ratio_threshold", 1.0f);
                b.field("tokens_weight_threshold", 0.5f);
            }
            b.endObject();
        }
        b.endObject();
    }

    protected void serializedMappingWithSomeIndexOptions(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.startObject("index_options");
        {
            b.field("prune", true);
            b.startObject("pruning_config");
            {
                b.field("tokens_freq_ratio_threshold", 3.0f);
                b.field("tokens_weight_threshold", TokenPruningConfig.DEFAULT_TOKENS_WEIGHT_THRESHOLD);
            }
            b.endObject();
        }
        b.endObject();
    }

    protected void minimalMappingWithSomeExplicitIndexOptions(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.startObject("index_options");
        {
            b.field("prune", true);
            b.startObject("pruning_config");
            {
                b.field("tokens_freq_ratio_threshold", 3.0f);
            }
            b.endObject();
        }
        b.endObject();
    }

    protected void mappingWithIndexOptionsOnlyPruneTrue(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.startObject("index_options");
        {
            b.field("prune", true);
        }
        b.endObject();
    }

    protected void mappingWithIndexOptionsPruneFalse(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.startObject("index_options");
        {
            b.field("prune", false);
        }
        b.endObject();
    }

    private void mapping(XContentBuilder b, @Nullable Boolean prune, PruningConfig pruningConfig) throws IOException {
        b.field("type", "sparse_vector");
        if (prune != null) {
            b.field("index_options", new SparseVectorFieldMapper.SparseVectorIndexOptions(prune, pruningConfig.tokenPruningConfig));
        }
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    private static int getFrequency(TokenStream tk) throws IOException {
        TermFrequencyAttribute freqAttribute = tk.addAttribute(TermFrequencyAttribute.class);
        tk.reset();
        assertTrue(tk.incrementToken());
        int freq = freqAttribute.getTermFrequency();
        assertFalse(tk.incrementToken());
        return freq;
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        @SuppressWarnings("unchecked")
        var expected = (Map<String, Float>) getSampleValueForDocument();
        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", expected)));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(expected.size(), fields.size());
        assertThat(fields.get(0), Matchers.instanceOf(FeatureField.class));

        for (IndexableField field : fields) {
            if (field instanceof FeatureField fField) {
                var value = expected.remove(fField.stringValue());
                assertThat(fField.getFeatureValue(), equalTo(value));
                int freq1 = getFrequency(fField.tokenStream(null, null));
                assertThat(XFeatureField.decodeFeatureValue(freq1), equalTo(value));
            }
        }
    }

    public void testDefaultsWithAndWithoutIncludeDefaults() throws Exception {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).mappingLookup().getMapper("field").toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();

        XContentBuilder withDefaults = JsonXContent.contentBuilder().startObject();
        withDefaults.startObject("field");
        minimalMappingWithExplicitDefaults(withDefaults);
        withDefaults.endObject();
        withDefaults.endObject();

        assertToXContentEquivalent(BytesReference.bytes(withDefaults), BytesReference.bytes(orig), XContentType.JSON);

        XContentBuilder origWithoutDefaults = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).mappingLookup()
            .getMapper("field")
            .toXContent(origWithoutDefaults, ToXContent.EMPTY_PARAMS);
        origWithoutDefaults.endObject();

        XContentBuilder withoutDefaults = JsonXContent.contentBuilder().startObject();
        withoutDefaults.startObject("field");
        minimalMapping(withoutDefaults);
        withoutDefaults.endObject();
        withoutDefaults.endObject();

        assertToXContentEquivalent(BytesReference.bytes(withoutDefaults), BytesReference.bytes(origWithoutDefaults), XContentType.JSON);
    }

    public void testDefaultsWithAndWithoutIncludeDefaultsOlderIndexVersion() throws Exception {
        IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            UPGRADE_TO_LUCENE_10_0_0,
            IndexVersionUtils.getPreviousVersion(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT)
        );

        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(indexVersion, fieldMapping(this::minimalMapping)).mappingLookup()
            .getMapper("field")
            .toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();

        XContentBuilder withDefaults = JsonXContent.contentBuilder().startObject();
        withDefaults.startObject("field");
        minimalFieldMappingPreviousIndexDefaultsIncluded(withDefaults);
        withDefaults.endObject();
        withDefaults.endObject();

        assertToXContentEquivalent(BytesReference.bytes(withDefaults), BytesReference.bytes(orig), XContentType.JSON);

        XContentBuilder origWithoutDefaults = JsonXContent.contentBuilder().startObject();
        createMapperService(indexVersion, fieldMapping(this::minimalMapping)).mappingLookup()
            .getMapper("field")
            .toXContent(origWithoutDefaults, ToXContent.EMPTY_PARAMS);
        origWithoutDefaults.endObject();

        XContentBuilder withoutDefaults = JsonXContent.contentBuilder().startObject();
        withoutDefaults.startObject("field");
        minimalMapping(withoutDefaults);
        withoutDefaults.endObject();
        withoutDefaults.endObject();

        assertToXContentEquivalent(BytesReference.bytes(withoutDefaults), BytesReference.bytes(origWithoutDefaults), XContentType.JSON);
    }

    public void testMappingWithExplicitIndexOptions() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMappingWithExplicitIndexOptions));
        assertEquals(Strings.toString(fieldMapping(this::minimalMappingWithExplicitIndexOptions)), mapper.mappingSource().toString());

        mapper = createDocumentMapper(fieldMapping(this::mappingWithIndexOptionsPruneFalse));
        assertEquals(Strings.toString(fieldMapping(this::mappingWithIndexOptionsPruneFalse)), mapper.mappingSource().toString());

        mapper = createDocumentMapper(fieldMapping(this::minimalMappingWithSomeExplicitIndexOptions));
        assertEquals(Strings.toString(fieldMapping(this::serializedMappingWithSomeIndexOptions)), mapper.mappingSource().toString());

        mapper = createDocumentMapper(fieldMapping(this::mappingWithIndexOptionsOnlyPruneTrue));
        assertEquals(Strings.toString(fieldMapping(this::mappingWithIndexOptionsOnlyPruneTrue)), mapper.mappingSource().toString());
    }

    public void testDotInFieldName() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDocument = mapper.parse(source(b -> b.field("field", Map.of("foo.bar", 10, "foobar", 20))));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertThat(fields.get(0), Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = null;
        FeatureField featureField2 = null;
        for (IndexableField field : fields) {
            if (field.stringValue().equals("foo.bar")) {
                featureField1 = (FeatureField) field;
            } else if (field.stringValue().equals("foobar")) {
                featureField2 = (FeatureField) field;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testHandlesMultiValuedFields() throws MapperParsingException, IOException {
        // setup a mapping that includes a sparse vector property
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "sparse_vector").endObject();
            b.startObject("foo").startObject("properties");
            {
                b.startObject("field").field("type", "sparse_vector").endObject();
            }
            b.endObject().endObject();
        }));

        // when providing a malformed list of values for a single field
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("foo", Arrays.asList(10, 20)).endObject()))
        );

        // then fail appropriately
        assertEquals(
            "[sparse_vector] fields take hashes that map a feature to a strictly positive float, but got unexpected token " + "START_ARRAY",
            e.getCause().getMessage()
        );

        // when providing a two fields with the same key name
        ParsedDocument doc1 = mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().startObject("field").field("coup", 1).endObject().endObject();
                b.startObject().startObject("field").field("bar", 5).endObject().endObject();
                b.startObject().startObject("field").field("bar", 20).endObject().endObject();
                b.startObject().startObject("field").field("bar", 10).endObject().endObject();
                b.startObject().startObject("field").field("soup", 2).endObject().endObject();
            }
            b.endArray();
        }));

        // then validate that the generate document stored both values appropriately and we have only the max value stored
        FeatureField barField = ((FeatureField) doc1.rootDoc().getByKey("foo.field\\.bar"));
        assertEquals(20, barField.getFeatureValue(), 1);

        FeatureField storedBarField = ((FeatureField) doc1.rootDoc().getFields("foo.field").get(1));
        assertEquals(20, storedBarField.getFeatureValue(), 1);

        assertEquals(3, doc1.rootDoc().getFields().stream().filter((f) -> f instanceof FeatureField).count());
    }

    public void testCannotBeUsedInMultiFields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("feature");
            b.field("type", "sparse_vector");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [feature] of type [sparse_vector] can't be used in multifields"));
    }

    public void testStoreIsNotUpdateable() throws IOException {
        var mapperService = createMapperService(fieldMapping(this::minimalMapping));
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "sparse_vector")
            .field("store", false)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        var exc = expectThrows(
            Exception.class,
            () -> mapperService.merge("_doc", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(exc.getMessage(), containsString("Cannot update parameter [store]"));
    }

    @SuppressWarnings("unchecked")
    public void testValueFetcher() throws Exception {
        for (boolean store : new boolean[] { true, false }) {
            var mapperService = createMapperService(fieldMapping(store ? this::minimalStoreMapping : this::minimalMapping));
            var mapper = mapperService.documentMapper();
            try (Directory directory = newDirectory()) {
                RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
                var sourceToParse = source(this::writeField);
                ParsedDocument doc1 = mapper.parse(sourceToParse);
                iw.addDocument(doc1.rootDoc());
                iw.close();
                try (DirectoryReader reader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                    LeafReader leafReader = getOnlyLeafReader(reader);
                    var searchContext = createSearchExecutionContext(mapperService, new IndexSearcher(leafReader));
                    var fieldType = mapper.mappers().getFieldType("field");
                    var valueFetcher = fieldType.valueFetcher(searchContext, null);
                    valueFetcher.setNextReader(leafReader.getContext());

                    var source = Source.fromBytes(sourceToParse.source());
                    var result = valueFetcher.fetchValues(source, 0, List.of());
                    assertThat(result.size(), equalTo(1));
                    assertThat(result.get(0), instanceOf(Map.class));
                    assertThat(toFloats((Map<String, ?>) result.get(0)), equalTo(toFloats((Map<String, ?>) source.source().get("field"))));
                }
            }
        }
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected boolean allowsNullValues() {
        return false;       // TODO should this allow null values?
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean syntheticSource) {
        return new SyntheticSourceSupport() {
            @Override
            public boolean preservesExactSource() {
                return false;
            }

            @Override
            public SyntheticSourceExample example(int maxValues) {
                var sample = getSampleValueForDocument();
                return new SyntheticSourceExample(sample, sample, b -> { minimalMapping(b); });
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() {
                return List.of();
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected String[] getParseMinimalWarnings(IndexVersion indexVersion) {
        String[] additionalWarnings = null;
        if (indexVersion.before(IndexVersions.V_8_0_0)) {
            additionalWarnings = new String[] { SparseVectorFieldMapper.ERROR_MESSAGE_7X };
        }
        return Strings.concatStringArrays(super.getParseMinimalWarnings(indexVersion), additionalWarnings);
    }

    @Override
    protected IndexVersion boostNotAllowedIndexVersion() {
        return NEW_SPARSE_VECTOR;
    }

    public void testSparseVectorUnsupportedIndex() {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.V_8_0_0,
            IndexVersions.FIRST_DETACHED_INDEX_VERSION
        );
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(version, fieldMapping(b -> {
            b.field("type", "sparse_vector");
        })));
        assertThat(e.getMessage(), containsString(SparseVectorFieldMapper.ERROR_MESSAGE_8X));
    }

    public void testPruneMustBeBoolean() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", "othervalue");
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("[index_options] failed to parse field [prune]"));
        assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getCause().getMessage(),
            containsString("Failed to parse value [othervalue] as only [true] or [false] are allowed.")
        );
    }

    public void testPruningConfigurationIsMap() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.field("pruning_config", "this_is_not_a_map");
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("[index_options] pruning_config doesn't support values of type:"));
        assertThat(e.getCause(), instanceOf(XContentParseException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("[index_options] pruning_config doesn't support values of type: VALUE_STRING")
        );
    }

    public void testWithIndexOptionsPruningConfigPruneRequired() throws Exception {

        Exception eTestPruneIsFalse = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", false);
            b.startObject("pruning_config");
            b.field("tokens_freq_ratio_threshold", 5.0);
            b.field("tokens_weight_threshold", 0.4);
            b.endObject();
            b.endObject();
        })));
        assertThat(eTestPruneIsFalse.getMessage(), containsString("[index_options] failed to parse field [pruning_config]"));
        assertThat(eTestPruneIsFalse.getCause().getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            eTestPruneIsFalse.getCause().getCause().getCause().getMessage(),
            containsString("[index_options] field [pruning_config] should only be set if [prune] is set to true")
        );

        Exception eTestPruneIsMissing = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.startObject("pruning_config");
            b.field("tokens_freq_ratio_threshold", 5.0);
            b.field("tokens_weight_threshold", 0.4);
            b.endObject();
            b.endObject();
        })));
        assertThat(
            eTestPruneIsMissing.getMessage(),
            containsString("Failed to parse mapping: Failed to build [index_options] after last required field arrived")
        );
        assertThat(eTestPruneIsMissing.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            eTestPruneIsMissing.getCause().getCause().getMessage(),
            containsString("[index_options] field [pruning_config] should only be set if [prune] is set to true")
        );
    }

    public void testTokensFreqRatioCorrect() {
        Exception eTestInteger = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.startObject("pruning_config");
            b.field("tokens_freq_ratio_threshold", "notaninteger");
            b.endObject();
            b.endObject();
        })));
        assertThat(
            eTestInteger.getMessage(),
            containsString("Failed to parse mapping: [0:0] [index_options] failed to parse field [pruning_config]")
        );
        assertThat(eTestInteger.getCause().getCause(), instanceOf(XContentParseException.class));
        assertThat(
            eTestInteger.getCause().getCause().getMessage(),
            containsString("[pruning_config] failed to parse field [tokens_freq_ratio_threshold]")
        );
        assertThat(eTestInteger.getCause().getCause().getCause(), instanceOf(NumberFormatException.class));
        assertThat(eTestInteger.getCause().getCause().getCause().getMessage(), containsString("For input string: \"notaninteger\""));

        Exception eTestRangeLower = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.startObject("pruning_config");
            b.field("tokens_freq_ratio_threshold", -2);
            b.endObject();
            b.endObject();
        })));
        assertThat(eTestRangeLower.getMessage(), containsString("[index_options] failed to parse field [pruning_config]"));
        assertThat(eTestRangeLower.getCause().getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            eTestRangeLower.getCause().getCause().getCause().getMessage(),
            containsString("[tokens_freq_ratio_threshold] must be between [1] and [100], got -2.0")
        );

        Exception eTestRangeHigher = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.startObject("pruning_config");
            b.field("tokens_freq_ratio_threshold", 101);
            b.endObject();
            b.endObject();
        })));
        assertThat(eTestRangeHigher.getMessage(), containsString("[index_options] failed to parse field [pruning_config]"));
        assertThat(eTestRangeHigher.getCause().getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            eTestRangeHigher.getCause().getCause().getCause().getMessage(),
            containsString("[tokens_freq_ratio_threshold] must be between [1] and [100], got 101.0")
        );
    }

    public void testTokensWeightThresholdCorrect() {
        Exception eTestDouble = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.startObject("pruning_config");
            b.field("tokens_weight_threshold", "notadouble");
            b.endObject();
            b.endObject();
        })));
        assertThat(eTestDouble.getMessage(), containsString("[index_options] failed to parse field [pruning_config]"));
        assertThat(eTestDouble.getCause().getCause().getCause(), instanceOf(NumberFormatException.class));
        assertThat(eTestDouble.getCause().getCause().getCause().getMessage(), containsString("For input string: \"notadouble\""));

        Exception eTestRangeLower = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.startObject("pruning_config");
            b.field("tokens_weight_threshold", -0.1);
            b.endObject();
            b.endObject();
        })));
        assertThat(eTestRangeLower.getMessage(), containsString("[index_options] failed to parse field [pruning_config]"));
        assertThat(eTestRangeLower.getCause().getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            eTestRangeLower.getCause().getCause().getCause().getMessage(),
            containsString("[tokens_weight_threshold] must be between 0 and 1")
        );

        Exception eTestRangeHigher = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "sparse_vector");
            b.startObject("index_options");
            b.field("prune", true);
            b.startObject("pruning_config");
            b.field("tokens_weight_threshold", 1.1);
            b.endObject();
            b.endObject();
        })));
        assertThat(eTestRangeHigher.getMessage(), containsString("[index_options] failed to parse field [pruning_config]"));
        assertThat(eTestRangeHigher.getCause().getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            eTestRangeHigher.getCause().getCause().getCause().getMessage(),
            containsString("[tokens_weight_threshold] must be between 0 and 1")
        );
    }

    private enum PruningScenario {
        NO_PRUNING,      // No pruning applied - all tokens preserved
        DEFAULT_PRUNING, // Default pruning configuration
        STRICT_PRUNING   // Stricter pruning with higher thresholds
    }

    private enum PruningConfig {
        NULL(null),
        EXPLICIT_DEFAULT(new TokenPruningConfig()),
        STRICT(new TokenPruningConfig(STRICT_TOKENS_FREQ_RATIO_THRESHOLD, STRICT_TOKENS_WEIGHT_THRESHOLD, false));

        public final @Nullable TokenPruningConfig tokenPruningConfig;

        PruningConfig(@Nullable TokenPruningConfig tokenPruningConfig) {
            this.tokenPruningConfig = tokenPruningConfig;
        }
    }

    private final Set<PruningOptions> validIndexPruningScenarios = Set.of(
        new PruningOptions(false, PruningConfig.NULL),
        new PruningOptions(true, PruningConfig.NULL),
        new PruningOptions(true, PruningConfig.EXPLICIT_DEFAULT),
        new PruningOptions(true, PruningConfig.STRICT),
        new PruningOptions(null, PruningConfig.NULL)
    );

    private record PruningOptions(@Nullable Boolean prune, PruningConfig pruningConfig) {}

    private void withSearchExecutionContext(MapperService mapperService, CheckedConsumer<SearchExecutionContext, IOException> consumer)
        throws IOException {
        var mapper = mapperService.documentMapper();
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);

            int commonDocs = 20;
            for (int i = 0; i < commonDocs; i++) {
                iw.addDocument(mapper.parse(source(b -> b.field("field", COMMON_TOKENS))).rootDoc());
            }

            int mediumDocs = 5;
            for (int i = 0; i < mediumDocs; i++) {
                iw.addDocument(mapper.parse(source(b -> b.field("field", MEDIUM_TOKENS))).rootDoc());
            }

            iw.addDocument(mapper.parse(source(b -> b.field("field", RARE_TOKENS))).rootDoc());

            // This will lower the averageTokenFreqRatio so that common tokens get pruned with default settings.
            // Depending on how the index is created, we will have 30-37 numUniqueTokens
            // this will result in an averageTokenFreqRatio of 0.1021 - 0.1259
            Map<String, Float> uniqueDoc = new TreeMap<>();
            for (int i = 0; i < 30; i++) {
                uniqueDoc.put("unique" + i, 0.5f);
            }
            iw.addDocument(mapper.parse(source(b -> b.field("field", uniqueDoc))).rootDoc());
            iw.close();

            try (DirectoryReader reader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                var searchContext = createSearchExecutionContext(mapperService, newSearcher(reader));
                consumer.accept(searchContext);
            }
        }
    }

    public void testPruningScenarios() throws Exception {
        for (int i = 0; i < 200; i++) {
            assertPruningScenario(
                randomFrom(validIndexPruningScenarios),
                new PruningOptions(randomFrom(true, false, null), randomFrom(PruningConfig.values()))
            );
        }
    }

    private XContentBuilder getIndexMapping(PruningOptions pruningOptions) throws IOException {
        return fieldMapping(b -> mapping(b, pruningOptions.prune, pruningOptions.pruningConfig));
    }

    private void assertQueryContains(List<Query> expectedClauses, Query query) {
        SparseVectorQueryWrapper queryWrapper = (SparseVectorQueryWrapper) query;
        var termsQuery = queryWrapper.getTermsQuery();
        assertNotNull(termsQuery);
        var booleanQuery = (BooleanQuery) termsQuery;

        Collection<Query> shouldClauses = booleanQuery.getClauses(BooleanClause.Occur.SHOULD);
        assertThat(shouldClauses, Matchers.containsInAnyOrder(expectedClauses.toArray()));
    }

    private PruningScenario getEffectivePruningScenario(
        PruningOptions indexPruningOptions,
        PruningOptions queryPruningOptions,
        IndexVersion indexVersion
    ) {
        Boolean shouldPrune = queryPruningOptions.prune;
        if (shouldPrune == null) {
            shouldPrune = indexPruningOptions.prune;
        }

        if (shouldPrune == null) {
            shouldPrune = indexVersion.between(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT_BACKPORT_8_X, UPGRADE_TO_LUCENE_10_0_0)
                || indexVersion.onOrAfter(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT);
        }

        PruningScenario pruningScenario = PruningScenario.NO_PRUNING;
        if (shouldPrune) {
            PruningConfig pruningConfig = queryPruningOptions.pruningConfig;
            if (pruningConfig == PruningConfig.NULL) {
                pruningConfig = indexPruningOptions.pruningConfig;
            }
            pruningScenario = switch (pruningConfig) {
                case STRICT -> PruningScenario.STRICT_PRUNING;
                case EXPLICIT_DEFAULT, NULL -> PruningScenario.DEFAULT_PRUNING;
            };
        }

        return pruningScenario;
    }

    private List<Query> getExpectedQueryClauses(
        SparseVectorFieldMapper.SparseVectorFieldType ft,
        PruningScenario pruningScenario,
        SearchExecutionContext searchExecutionContext
    ) {
        List<WeightedToken> tokens = switch (pruningScenario) {
            case NO_PRUNING -> QUERY_VECTORS;
            case DEFAULT_PRUNING -> QUERY_VECTORS.stream()
                .filter(t -> t.token().startsWith("rare") || t.token().startsWith("medium"))
                .toList();
            case STRICT_PRUNING -> QUERY_VECTORS.stream().filter(t -> t.token().endsWith("keep_strict")).toList();
        };

        return tokens.stream().map(t -> {
            Query termQuery = ft.termQuery(t.token(), searchExecutionContext);
            return new BoostQuery(termQuery, t.weight());
        }).collect(Collectors.toUnmodifiableList());
    }

    private void assertPruningScenario(PruningOptions indexPruningOptions, PruningOptions queryPruningOptions) throws IOException {
        IndexVersion indexVersion = getIndexVersion();
        MapperService mapperService = createMapperService(indexVersion, getIndexMapping(indexPruningOptions));
        PruningScenario effectivePruningScenario = getEffectivePruningScenario(indexPruningOptions, queryPruningOptions, indexVersion);
        withSearchExecutionContext(mapperService, (context) -> {
            SparseVectorFieldMapper.SparseVectorFieldType ft = (SparseVectorFieldMapper.SparseVectorFieldType) mapperService.fieldType(
                "field"
            );
            List<Query> expectedQueryClauses = getExpectedQueryClauses(ft, effectivePruningScenario, context);
            Query finalizedQuery = ft.finalizeSparseVectorQuery(
                context,
                "field",
                QUERY_VECTORS,
                queryPruningOptions.prune,
                queryPruningOptions.pruningConfig.tokenPruningConfig
            );
            assertQueryContains(expectedQueryClauses, finalizedQuery);
        });
    }

    private static IndexVersion getIndexVersion() {
        VersionRange versionRange = randomFrom(VersionRange.values());
        return versionRange.getRandomVersion();
    }

    private enum VersionRange {
        ES_V8X_WITHOUT_INDEX_OPTIONS_SUPPORT(
            NEW_SPARSE_VECTOR,
            IndexVersionUtils.getPreviousVersion(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT_BACKPORT_8_X)
        ),
        ES_V8X_WITH_INDEX_OPTIONS_SUPPORT(
            SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT_BACKPORT_8_X,
            IndexVersionUtils.getPreviousVersion(UPGRADE_TO_LUCENE_10_0_0)
        ),
        ES_V9X_WITHOUT_INDEX_OPTIONS_SUPPORT(
            UPGRADE_TO_LUCENE_10_0_0,
            IndexVersionUtils.getPreviousVersion(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT)
        ),
        ES_V9X_WITH_INDEX_OPTIONS_SUPPORT(SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT, IndexVersion.current());

        private final IndexVersion fromVersion;
        private final IndexVersion toVersion;

        VersionRange(IndexVersion fromVersion, IndexVersion toVersion) {
            this.fromVersion = fromVersion;
            this.toVersion = toVersion;
        }

        IndexVersion getRandomVersion() {
            // TODO: replace implementation with `IndexVersionUtils::randomVersionBetween` once support is added
            // for handling unbalanced version distributions.
            NavigableSet<IndexVersion> allReleaseVersions = IndexVersionUtils.allReleasedVersions();
            Set<IndexVersion> candidateVersions = allReleaseVersions.subSet(fromVersion, toVersion);
            return ESTestCase.randomFrom(candidateVersions);
        }
    }

    private static final List<WeightedToken> QUERY_VECTORS = Stream.of(RARE_TOKENS, MEDIUM_TOKENS, COMMON_TOKENS)
        .flatMap(map -> map.entrySet().stream())
        .map(entry -> new WeightedToken(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());

    /**
     * Handles float/double conversion when reading/writing with xcontent by converting all numbers to floats.
     */
    private Map<String, Float> toFloats(Map<String, ?> value) {
        // preserve order
        Map<String, Float> result = new LinkedHashMap<>();
        for (var entry : value.entrySet()) {
            if (entry.getValue() instanceof Number num) {
                result.put(entry.getKey(), num.floatValue());
            } else {
                throw new IllegalArgumentException("Expected Number, got: " + value.getClass().getSimpleName());
            }
        }
        return result;
    }

    public static IndexVersion getIndexOptionsCompatibleIndexVersion() {
        return IndexVersionUtils.randomVersionBetween(random(), SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT, IndexVersion.current());
    }
}
