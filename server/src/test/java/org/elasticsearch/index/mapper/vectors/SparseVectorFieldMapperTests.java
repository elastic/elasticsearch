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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.hamcrest.Matchers;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.IndexVersions.UPGRADE_TO_LUCENE_10_0_0;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.NEW_SPARSE_VECTOR_INDEX_VERSION;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.PREVIOUS_SPARSE_VECTOR_INDEX_VERSION;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SparseVectorFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        Map<String, Float> map = new LinkedHashMap<>();
        map.put("ten", 10f);
        map.put("twenty", 20f);
        return map;
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
    }

    protected void minimalMappingWithExplicitIndexOptions(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
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

    protected void mappingWithIndexOptionsPruneFalse(XContentBuilder b) throws IOException {
        b.field("type", "sparse_vector");
        b.startObject("index_options");
        {
            b.field("prune", false);
        }
        b.endObject();
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
        assertEquals(Strings.toString(fieldMapping(this::minimalMappingWithExplicitIndexOptions)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(this::writeField));

        List<IndexableField> fields = doc1.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertThat(fields.get(0), Matchers.instanceOf(XFeatureField.class));
        XFeatureField featureField1 = null;
        XFeatureField featureField2 = null;
        for (IndexableField field : fields) {
            if (field.stringValue().equals("ten")) {
                featureField1 = (XFeatureField) field;
            } else if (field.stringValue().equals("twenty")) {
                featureField2 = (XFeatureField) field;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        int freq1 = getFrequency(featureField1.tokenStream(null, null));
        int freq2 = getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testMappingWithoutIndexOptionsUsesDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMappingWithExplicitIndexOptions)), mapper.mappingSource().toString());

        IndexVersion preIndexOptionsVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            UPGRADE_TO_LUCENE_10_0_0,
            SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_VERSION
        );
        DocumentMapper previousMapper = createDocumentMapper(preIndexOptionsVersion, fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), previousMapper.mappingSource().toString());
    }

    public void testDotInFieldName() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDocument = mapper.parse(source(b -> b.field("field", Map.of("foo.bar", 10, "foobar", 20))));

        List<IndexableField> fields = parsedDocument.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertThat(fields.get(0), Matchers.instanceOf(XFeatureField.class));
        XFeatureField featureField1 = null;
        XFeatureField featureField2 = null;
        for (IndexableField field : fields) {
            if (field.stringValue().equals("foo.bar")) {
                featureField1 = (XFeatureField) field;
            } else if (field.stringValue().equals("foobar")) {
                featureField2 = (XFeatureField) field;
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
        XFeatureField barField = ((XFeatureField) doc1.rootDoc().getByKey("foo.field\\.bar"));
        assertEquals(20, barField.getFeatureValue(), 1);

        XFeatureField storedBarField = ((XFeatureField) doc1.rootDoc().getFields("foo.field").get(1));
        assertEquals(20, storedBarField.getFeatureValue(), 1);

        assertEquals(3, doc1.rootDoc().getFields().stream().filter((f) -> f instanceof XFeatureField).count());
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
            .field("store", true)
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
        boolean withStore = randomBoolean();
        return new SyntheticSourceSupport() {
            @Override
            public boolean preservesExactSource() {
                return withStore == false;
            }

            @Override
            public SyntheticSourceExample example(int maxValues) {
                return new SyntheticSourceExample(getSampleValueForDocument(), getSampleValueForDocument(), b -> {
                    if (withStore) {
                        minimalStoreMapping(b);
                    } else {
                        minimalMapping(b);
                    }
                });
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
        if (indexVersion.before(PREVIOUS_SPARSE_VECTOR_INDEX_VERSION)) {
            additionalWarnings = new String[] { SparseVectorFieldMapper.ERROR_MESSAGE_7X };
        }
        return Strings.concatStringArrays(super.getParseMinimalWarnings(indexVersion), additionalWarnings);
    }

    @Override
    protected IndexVersion boostNotAllowedIndexVersion() {
        return NEW_SPARSE_VECTOR_INDEX_VERSION;
    }

    public void testSparseVectorUnsupportedIndex() {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            random(),
            PREVIOUS_SPARSE_VECTOR_INDEX_VERSION,
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

    private void withSearchExecutionContext(MapperService mapperService, CheckedConsumer<SearchExecutionContext, IOException> consumer)
        throws IOException {
        for (boolean store : new boolean[] { true, false }) {
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
                    consumer.accept(searchContext);
                }
            }
        }
    }

    public void testTypeQueryFinalizationWithRandomOptions() throws Exception {
        for (int i = 0; i < 20; i++) {
            runTestTypeQueryFinalization(
                randomBoolean(), // usePreviousIndex
                randomBoolean(), // useIndexOptionsDefaults
                randomBoolean(), // explicitIndexOptionsDoNotPrune
                randomBoolean(), // queryOverridesPruning
                randomBoolean()  // queryOverrideExplicitFalse
            );
        }
    }

    public void testTypeQueryFinalizationDefaultsCurrentVersion() throws Exception {
        IndexVersion version = IndexVersion.current();
        MapperService mapperService = createMapperService(version, fieldMapping(this::minimalMapping));

        // query should be pruned by default on newer index versions
        performTypeQueryFinalizationTest(mapperService, null, null, null, true);
    }

    public void testTypeQueryFinalizationDefaultsPreviousVersion() throws Exception {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.UPGRADE_TO_LUCENE_10_2_1,
            IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT
        );
        MapperService mapperService = createMapperService(version, fieldMapping(this::minimalMapping));

        // query should _not_ be pruned by default on older index versions
        performTypeQueryFinalizationTest(mapperService, null, null, null, false);
    }

    public void testTypeQueryFinalizationWithIndexExplicit() throws Exception {
        IndexVersion version = IndexVersion.current();
        MapperService mapperService = createMapperService(version, fieldMapping(this::minimalMapping));

        // query should be pruned via explicit index options
        performTypeQueryFinalizationTest(
            mapperService,
            new SparseVectorFieldMapper.IndexOptions(true, new TokenPruningConfig()),
            null,
            null,
            true
        );
    }

    public void testTypeQueryFinalizationWithIndexExplicitDoNotPrune() throws Exception {
        IndexVersion version = IndexVersion.current();
        MapperService mapperService = createMapperService(version, fieldMapping(this::mappingWithIndexOptionsPruneFalse));

        // query should be pruned via explicit index options
        performTypeQueryFinalizationTest(mapperService, new SparseVectorFieldMapper.IndexOptions(false, null), null, null, false);
    }

    public void testTypeQueryFinalizationQueryOverridesPruning() throws Exception {
        IndexVersion version = IndexVersion.current();
        MapperService mapperService = createMapperService(version, fieldMapping(this::mappingWithIndexOptionsPruneFalse));

        // query should still be pruned due to query builder setting it
        performTypeQueryFinalizationTest(
            mapperService,
            new SparseVectorFieldMapper.IndexOptions(false, null),
            true,
            new TokenPruningConfig(),
            true
        );
    }

    public void testTypeQueryFinalizationQueryOverridesPruningOff() throws Exception {
        IndexVersion version = IndexVersion.current();
        MapperService mapperService = createMapperService(version, fieldMapping(this::mappingWithIndexOptionsPruneFalse));

        // query should not pruned due to query builder setting it
        performTypeQueryFinalizationTest(
            mapperService,
            new SparseVectorFieldMapper.IndexOptions(true, new TokenPruningConfig()),
            false,
            null,
            false
        );
    }

    private void performTypeQueryFinalizationTest(
        MapperService mapperService,
        SparseVectorFieldMapper.IndexOptions indexOptions,
        @Nullable Boolean queryPrune,
        @Nullable TokenPruningConfig queryTokenPruningConfig,
        boolean queryShouldBePruned
    ) throws IOException {
        withSearchExecutionContext(mapperService, (context) -> {
            SparseVectorFieldMapper.SparseVectorFieldType ft = new SparseVectorFieldMapper.SparseVectorFieldType(
                "field",
                false,
                Collections.emptyMap(),
                indexOptions
            );
            Query finalizedQuery = ft.finalizeSparseVectorQuery(context, "field", QUERY_VECTORS, queryPrune, queryTokenPruningConfig);

            if (queryShouldBePruned) {
                assertQueryWasPruned(finalizedQuery);
            } else {
                assertQueryWasNotPruned(finalizedQuery);
            }
        });
    }

    private void assertQueryWasPruned(Query query) {
        assertQueryHasClauseCount(query, 0);
    }

    private void assertQueryWasNotPruned(Query query) {
        assertQueryHasClauseCount(query, QUERY_VECTORS.size());
    }

    private void assertQueryHasClauseCount(Query query, int clauseCount) {
        SparseVectorQueryWrapper queryWrapper = (SparseVectorQueryWrapper) query;
        var termsQuery = queryWrapper.getTermsQuery();
        assertNotNull(termsQuery);
        var booleanQuery = (BooleanQuery) termsQuery;
        Collection<Query> clauses = booleanQuery.getClauses(BooleanClause.Occur.SHOULD);
        assertThat(clauses.size(), equalTo(clauseCount));
    }

    private void runTestTypeQueryFinalization(
        boolean usePreviousIndex,
        boolean useIndexOptionsDefaults,
        boolean explicitIndexOptionsDoNotPrune,
        boolean queryOverridesPruning,
        boolean queryOverrideExplicitFalse
    ) throws IOException {
        IndexVersion version = usePreviousIndex
            ? IndexVersionUtils.randomVersionBetween(
                random(),
                IndexVersions.UPGRADE_TO_LUCENE_10_2_1,
                IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT
            )
            : IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT;

        MapperService mapperService = getMapperServiceForRandomizedFinalizationTest(
            version,
            useIndexOptionsDefaults,
            explicitIndexOptionsDoNotPrune
        );

        SparseVectorFieldMapper.IndexOptions indexOptions = getIndexOptionsQueryFinalization(
            usePreviousIndex,
            useIndexOptionsDefaults,
            explicitIndexOptionsDoNotPrune
        );

        Boolean shouldQueryPrune = queryOverridesPruning ? (queryOverrideExplicitFalse == false) : null;

        TokenPruningConfig queryPruningConfig = queryOverridesPruning && queryOverrideExplicitFalse == false
            ? new TokenPruningConfig()
            : null;

        boolean resultShouldBePruned = true;
        if (queryOverridesPruning && queryOverrideExplicitFalse) {
            resultShouldBePruned = false;
        } else if (queryOverridesPruning == false && (usePreviousIndex || explicitIndexOptionsDoNotPrune)) {
            resultShouldBePruned = false;
        }

        try {
            performTypeQueryFinalizationTest(mapperService, indexOptions, shouldQueryPrune, queryPruningConfig, resultShouldBePruned);
        } catch (AssertionError e) {
            String message = "performTypeQueryFinalizationTest failed using parameters: "
                + "usePreviousIndex: "
                + usePreviousIndex
                + ", useIndexOptionsDefaults: "
                + useIndexOptionsDefaults
                + ", explicitIndexOptionsDoNotPrune: "
                + explicitIndexOptionsDoNotPrune
                + ", queryOverridesPruning: "
                + queryOverridesPruning
                + ", queryOverrideExplicitFalse: "
                + queryOverrideExplicitFalse;
            throw new AssertionError(message, e);
        }

    }

    private SparseVectorFieldMapper.IndexOptions getIndexOptionsQueryFinalization(
        boolean usePreviousIndex,
        boolean useIndexOptionsDefaults,
        boolean explicitIndexOptionsDoNotPrune
    ) {
        if (usePreviousIndex) {
            return null;
        }

        if (useIndexOptionsDefaults && explicitIndexOptionsDoNotPrune == false) {
            return null;
        }

        return explicitIndexOptionsDoNotPrune
            ? new SparseVectorFieldMapper.IndexOptions(false, null)
            : new SparseVectorFieldMapper.IndexOptions(true, new TokenPruningConfig());
    }

    private MapperService getMapperServiceForRandomizedFinalizationTest(
        IndexVersion indexVersion,
        boolean useIndexOptionsDefaults,
        boolean explicitIndexOptionsDoNotPrune
    ) throws IOException {
        if (useIndexOptionsDefaults) {
            return createMapperService(indexVersion, fieldMapping(this::minimalMapping));
        }

        if (explicitIndexOptionsDoNotPrune) {
            return createMapperService(indexVersion, fieldMapping(this::mappingWithIndexOptionsPruneFalse));
        }

        return createMapperService(indexVersion, fieldMapping(this::minimalMapping));
    }

    private static List<WeightedToken> QUERY_VECTORS = List.of(
        new WeightedToken("pugs", 0.5f),
        new WeightedToken("cats", 0.4f),
        new WeightedToken("is", 0.1f)
    );

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
}
