/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.Level;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Set;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Mapper}s.
 */
public abstract class MapperTestCase extends MapperServiceTestCase {
    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    /**
     * Writes the field and a sample value for it to the provided {@link XContentBuilder}.
     * To be overridden in case the field should not be written at all in documents,
     * like in the case of runtime fields.
     */
    protected void writeField(XContentBuilder builder) throws IOException {
        builder.field("field");
        builder.value(getSampleValueForDocument());
    }

    /**
     * Returns a sample value for the field, to be used in a document
     */
    protected abstract Object getSampleValueForDocument();

    /**
     * Returns a sample value for the field, to be used when querying the field. Normally this is the same format as
     * what is indexed as part of a document, and returned by {@link #getSampleValueForDocument()}, but there
     * are cases where fields are queried differently frow how they are indexed e.g. token_count or runtime fields
     */
    protected Object getSampleValueForQuery() {
        return getSampleValueForDocument();
    }

    /**
     * This test verifies that the exists query created is the appropriate one, and aligns with the data structures
     * being created for a document with a value for the field. This can only be verified for the minimal mapping.
     * Field types that allow configurable doc_values or norms should write their own tests that creates the different
     * mappings combinations and invoke {@link #assertExistsQuery(MapperService)} to verify the behaviour.
     */
    public final void testExistsQueryMinimalMapping() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    protected void assertExistsQuery(MapperService mapperService) throws IOException {
        LuceneDocument fields = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
        MappedFieldType fieldType = mapperService.fieldType("field");
        Query query = fieldType.existsQuery(searchExecutionContext);
        assertExistsQuery(fieldType, query, fields);
    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        if (fieldType.hasDocValues()) {
            assertThat(query, instanceOf(DocValuesFieldExistsQuery.class));
            DocValuesFieldExistsQuery fieldExistsQuery = (DocValuesFieldExistsQuery) query;
            assertEquals("field", fieldExistsQuery.getField());
            assertDocValuesField(fields, "field");
            assertNoFieldNamesField(fields);
        } else if (fieldType.getTextSearchInfo().hasNorms()) {
            assertThat(query, instanceOf(NormsFieldExistsQuery.class));
            NormsFieldExistsQuery normsFieldExistsQuery = (NormsFieldExistsQuery) query;
            assertEquals("field", normsFieldExistsQuery.getField());
            assertHasNorms(fields, "field");
            assertNoDocValuesField(fields, "field");
            assertNoFieldNamesField(fields);
        } else {
            assertThat(query, instanceOf(TermQuery.class));
            TermQuery termQuery = (TermQuery) query;
            assertEquals(FieldNamesFieldMapper.NAME, termQuery.getTerm().field());
            // we always perform a term query against _field_names, even when the field
            // is not added to _field_names because it is not indexed nor stored
            assertEquals("field", termQuery.getTerm().text());
            assertNoDocValuesField(fields, "field");
            if (fieldType.isSearchable() || fieldType.isStored()) {
                assertNotNull(fields.getField(FieldNamesFieldMapper.NAME));
            } else {
                assertNoFieldNamesField(fields);
            }
        }
    }

    protected static void assertNoFieldNamesField(LuceneDocument fields) {
        assertNull(fields.getField(FieldNamesFieldMapper.NAME));
    }

    protected static void assertHasNorms(LuceneDocument doc, String field) {
        IndexableField[] fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            IndexableFieldType indexableFieldType = indexableField.fieldType();
            if (indexableFieldType.indexOptions() != IndexOptions.NONE) {
                assertFalse(indexableFieldType.omitNorms());
                return;
            }
        }
        fail("field [" + field + "] should be indexed but it isn't");
    }

    protected static void assertDocValuesField(LuceneDocument doc, String field) {
        IndexableField[] fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            if (indexableField.fieldType().docValuesType().equals(DocValuesType.NONE) == false) {
                return;
            }
        }
        fail("doc_values not present for field [" + field + "]");
    }

    protected static void assertNoDocValuesField(LuceneDocument doc, String field) {
        IndexableField[] fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            assertEquals(DocValuesType.NONE, indexableField.fieldType().docValuesType());
        }
    }

    protected <T> void assertDimension(boolean isDimension, Function<T, Boolean> checker) throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", isDimension);
        }));

        @SuppressWarnings("unchecked") // Syntactic sugar in tests
        T fieldType = (T) mapperService.fieldType("field");
        assertThat(checker.apply(fieldType), equalTo(isDimension));
    }

    protected <T> void assertMetricType(String metricType, Function<T, Enum<TimeSeriesParams.MetricType>> checker) throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", metricType);
        }));

        @SuppressWarnings("unchecked") // Syntactic sugar in tests
        T fieldType = (T) mapperService.fieldType("field");
        assertThat(checker.apply(fieldType).name(), equalTo(metricType));
    }

    public final void testEmptyName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
        assertParseMinimalWarnings();
    }

    public final void testMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMinimalWarnings();
    }

    // TODO make this final once we remove FieldMapperTestCase2
    public void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, INCLUDE_DEFAULTS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMaximalWarnings();
    }

    protected Level getWarningLevel() {
        return DeprecationLogger.CRITICAL;
    }

    protected final void assertParseMinimalWarnings() {
        String[] warnings = getParseMinimalWarnings();
        if (warnings.length > 0) {
            assertWarnings(getWarningLevel(), warnings);
        }
    }

    protected final void assertParseMaximalWarnings() {
        String[] warnings = getParseMaximalWarnings();
        if (warnings.length > 0) {
            assertWarnings(getWarningLevel(), warnings);
        }
    }

    protected String[] getParseMinimalWarnings() {
        // Most mappers don't emit any warnings
        return Strings.EMPTY_ARRAY;
    }

    protected String[] getParseMaximalWarnings() {
        // Most mappers don't emit any warnings
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Override to disable testing {@code meta} in fields that don't support it.
     */
    protected boolean supportsMeta() {
        return true;
    }

    protected void metaMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    public void testMeta() throws IOException {
        assumeTrue("Field doesn't support meta", supportsMeta());
        XContentBuilder mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("foo", "bar"));
        });
        MapperService mapperService = createMapperService(mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(this::metaMapping);
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("baz", "quux"));
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    protected String typeName() throws IOException {
        MapperService ms = createMapperService(fieldMapping(this::minimalMapping));
        return ms.fieldType("field").typeName();
    }

    public final void testDeprecatedBoost() throws IOException {
        MapperService ms = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("boost", 2.0);
        }));
        String type = typeName();
        String[] warnings = Strings.concatStringArrays(
            getParseMinimalWarnings(),
            new String[] {
                "Parameter [boost] on field [field] is deprecated and will be removed in 8.0",
                "Parameter [boost] has no effect on type [" + type + "] and will be removed in future" }
        );
        allowedWarnings(warnings);
    }

    /**
     * Use a {@linkplain ValueFetcher} to extract values from doc values.
     */
    protected final List<?> fetchFromDocValues(MapperService mapperService, MappedFieldType ft, DocValueFormat format, Object sourceValue)
        throws IOException {

        SetOnce<List<?>> result = new SetOnce<>();
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field(ft.name(), sourceValue))).rootDoc());
        }, iw -> {
            SearchLookup lookup = new SearchLookup(mapperService::fieldType, fieldDataLookup());
            ValueFetcher valueFetcher = new DocValueFetcher(format, lookup.getForField(ft));
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            lookup.source().setSegmentAndDocument(context, 0);
            valueFetcher.setNextReader(context);
            result.set(valueFetcher.fetchValues(lookup.source(), new ArrayList<>()));
        });
        return result.get();
    }

    private class UpdateCheck {
        final XContentBuilder init;
        final XContentBuilder update;
        final Consumer<FieldMapper> check;

        private UpdateCheck(CheckedConsumer<XContentBuilder, IOException> update, Consumer<FieldMapper> check) throws IOException {
            this.init = fieldMapping(MapperTestCase.this::minimalMapping);
            this.update = fieldMapping(b -> {
                minimalMapping(b);
                update.accept(b);
            });
            this.check = check;
        }

        private UpdateCheck(
            CheckedConsumer<XContentBuilder, IOException> init,
            CheckedConsumer<XContentBuilder, IOException> update,
            Consumer<FieldMapper> check
        ) throws IOException {
            this.init = fieldMapping(init);
            this.update = fieldMapping(update);
            this.check = check;
        }
    }

    private static class ConflictCheck {
        final XContentBuilder init;
        final XContentBuilder update;

        private ConflictCheck(XContentBuilder init, XContentBuilder update) {
            this.init = init;
            this.update = update;
        }
    }

    public class ParameterChecker {

        List<UpdateCheck> updateChecks = new ArrayList<>();
        Map<String, ConflictCheck> conflictChecks = new HashMap<>();

        /**
         * Register a check that a parameter can be updated, using the minimal mapping as a base
         *
         * @param update a field builder applied on top of the minimal mapping
         * @param check  a check that the updated parameter has been applied to the FieldMapper
         */
        public void registerUpdateCheck(CheckedConsumer<XContentBuilder, IOException> update, Consumer<FieldMapper> check)
            throws IOException {
            updateChecks.add(new UpdateCheck(update, check));
        }

        /**
         * Register a check that a parameter can be updated
         *
         * @param init   the initial mapping
         * @param update the updated mapping
         * @param check  a check that the updated parameter has been applied to the FieldMapper
         */
        public void registerUpdateCheck(
            CheckedConsumer<XContentBuilder, IOException> init,
            CheckedConsumer<XContentBuilder, IOException> update,
            Consumer<FieldMapper> check
        ) throws IOException {
            updateChecks.add(new UpdateCheck(init, update, check));
        }

        /**
         * Register a check that a parameter update will cause a conflict, using the minimal mapping as a base
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param update a field builder applied on top of the minimal mapping
         */
        public void registerConflictCheck(String param, CheckedConsumer<XContentBuilder, IOException> update) throws IOException {
            conflictChecks.put(param, new ConflictCheck(fieldMapping(MapperTestCase.this::minimalMapping), fieldMapping(b -> {
                minimalMapping(b);
                update.accept(b);
            })));
        }

        /**
         * Register a check that a parameter update will cause a conflict
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param init   the initial mapping
         * @param update the updated mapping
         */
        public void registerConflictCheck(String param, XContentBuilder init, XContentBuilder update) {
            conflictChecks.put(param, new ConflictCheck(init, update));
        }
    }

    protected abstract void registerParameters(ParameterChecker checker) throws IOException;

    public void testUpdates() throws IOException {
        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);
        for (UpdateCheck updateCheck : checker.updateChecks) {
            MapperService mapperService = createMapperService(updateCheck.init);
            merge(mapperService, updateCheck.update);
            FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            updateCheck.check.accept(mapper);
            // do it again to ensure that we don't get conflicts the second time
            merge(mapperService, updateCheck.update);
            mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            updateCheck.check.accept(mapper);

        }
        for (String param : checker.conflictChecks.keySet()) {
            MapperService mapperService = createMapperService(checker.conflictChecks.get(param).init);
            // merging the same change is fine
            merge(mapperService, checker.conflictChecks.get(param).init);
            // merging the conflicting update should throw an exception
            Exception e = expectThrows(
                IllegalArgumentException.class,
                "No conflict when updating parameter [" + param + "]",
                () -> merge(mapperService, checker.conflictChecks.get(param).update)
            );
            assertThat(
                e.getMessage(),
                anyOf(containsString("Cannot update parameter [" + param + "]"), containsString("different [" + param + "]"))
            );
        }
        assertParseMaximalWarnings();
    }

    public final void testTextSearchInfoConsistency() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = mapperService.fieldType("field");
        if (fieldType.getTextSearchInfo() == TextSearchInfo.NONE) {
            expectThrows(IllegalArgumentException.class, () -> fieldType.termQuery(null, null));
        } else {
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
            assertNotNull(fieldType.termQuery(getSampleValueForQuery(), searchExecutionContext));
        }
        assertSearchable(fieldType);
        assertParseMinimalWarnings();
    }

    protected void assertSearchable(MappedFieldType fieldType) {
        assertEquals(fieldType.isSearchable(), fieldType.getTextSearchInfo() != TextSearchInfo.NONE);
    }

    /**
     * Asserts that fetching a single value from doc values and from the native
     * {@link MappedFieldType#valueFetcher} produce the same results.
     * <p>
     * Generally this method covers many many random cases but rarely. So if
     * it fails its generally a good idea to capture its randomized
     * parameters into a new method so we can be sure we consistently test
     * any unique and interesting failure case. See the tests for
     * {@link DateFieldMapper} for some examples.
     */
    public final void testFetch() throws IOException {
        MapperService mapperService = randomFetchTestMapper();
        try {
            MappedFieldType ft = mapperService.fieldType("field");
            assertFetch(mapperService, "field", generateRandomInputValue(ft), randomFetchTestFormat());
        } finally {
            assertParseMinimalWarnings();
        }
    }

    /**
     * Asserts that fetching many values from doc values and from the native
     * {@link MappedFieldType#valueFetcher} produce the same results.
     * <p>
     * Generally this method covers many many random cases but rarely. So if
     * it fails its generally a good idea to capture its randomized
     * parameters into a new method so we can be sure we consistently test
     * any unique and interesting failure case. See the tests for
     * {@link DateFieldMapper} for some examples.
     */
    public final void testFetchMany() throws IOException {
        MapperService mapperService = randomFetchTestMapper();
        try {
            MappedFieldType ft = mapperService.fieldType("field");
            int count = between(2, 10);
            List<Object> values = new ArrayList<>(count);
            while (values.size() < count) {
                values.add(generateRandomInputValue(ft));
            }
            assertFetch(mapperService, "field", values, randomFetchTestFormat());
        } finally {
            assertParseMinimalWarnings();
        }
    }

    protected final MapperService randomFetchTestMapper() throws IOException {
        return createMapperService(mapping(b -> {
            b.startObject("field");
            randomFetchTestFieldConfig(b);
            b.endObject();
        }));
    }

    /**
     * Field configuration for {@link #testFetch} and {@link #testFetchMany}.
     * Default implementation delegates to {@link #minimalMapping} but can
     * be overridden to randomize the field type and options.
     */
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    /**
     * A random format to use when tripping in {@link #testFetch} and
     * {@link #testFetchMany}.
     */
    protected String randomFetchTestFormat() {
        return null;
    }

    /**
     * Test that dimension parameter is not updateable
     */
    protected void registerDimensionChecks(ParameterChecker checker) throws IOException {
        // dimension cannot be updated
        checker.registerConflictCheck("time_series_dimension", b -> b.field("time_series_dimension", true));
        checker.registerConflictCheck("time_series_dimension", b -> b.field("time_series_dimension", false));
        checker.registerConflictCheck("time_series_dimension", fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", false);
        }), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }));
        checker.registerConflictCheck("time_series_dimension", fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", false);
        }));
    }

    /**
     * Create a random {@code _source} value for this field. Must be compatible
     * with {@link XContentBuilder#value(Object)} and the field's parser.
     */
    protected abstract Object generateRandomInputValue(MappedFieldType ft);

    /**
     * Assert that fetching a value using {@link MappedFieldType#valueFetcher}
     * produces the same value as fetching using doc values.
     */
    protected void assertFetch(MapperService mapperService, String field, Object value, String format) throws IOException {
        MappedFieldType ft = mapperService.fieldType(field);
        SourceToParse source = source(b -> b.field(ft.name(), value));
        ValueFetcher docValueFetcher = new DocValueFetcher(
            ft.docValueFormat(format, null),
            ft.fielddataBuilder("test", () -> null).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
        );
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft)).thenAnswer(inv -> {
            return fieldDataLookup().apply(ft, () -> { throw new UnsupportedOperationException(); });
        });
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            SourceLookup sourceLookup = new SourceLookup();
            sourceLookup.setSegmentAndDocument(ir.leaves().get(0), 0);
            docValueFetcher.setNextReader(ir.leaves().get(0));
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromDocValues = docValueFetcher.fetchValues(sourceLookup, new ArrayList<>());
            List<Object> fromNative = nativeFetcher.fetchValues(sourceLookup, new ArrayList<>());
            /*
             * The native fetcher uses byte, short, etc but doc values always
             * uses long or double. This difference is fine because on the outside
             * users can't see it.
             */
            fromNative = fromNative.stream().map(o -> {
                if (o instanceof Integer || o instanceof Short || o instanceof Byte) {
                    return ((Number) o).longValue();
                }
                if (o instanceof Float) {
                    return ((Float) o).doubleValue();
                }
                return o;
            }).collect(toList());

            if (dedupAfterFetch()) {
                fromNative = fromNative.stream().distinct().collect(Collectors.toList());
            }
            /*
             * Doc values sort according to something appropriate to the field
             * and the native fetchers usually don't sort. We're ok with this
             * difference. But we have to convince the test we're ok with it.
             */
            assertThat("fetching " + value, fromNative, containsInAnyOrder(fromDocValues.toArray()));
        });
    }

    /**
     * A few field types (e.g. keyword fields) don't allow duplicate values, so in those cases we need to de-dup our expected values.
     * Field types where this is the case should overwrite this. The default is to not de-duplicate though.
     */
    protected boolean dedupAfterFetch() {
        return false;
    }

    /**
     * @return whether or not this field type supports access to its values from a SearchLookup
     */
    protected boolean supportsSearchLookup() {
        return true;
    }

    /**
     * Checks that field data from this field produces the same values for query-time
     * scripts and for index-time scripts
     */
    public final void testIndexTimeFieldData() throws IOException {
        assumeTrue("Field type does not support access via search lookup", supportsSearchLookup());
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertParseMinimalWarnings();
        MappedFieldType fieldType = mapperService.fieldType("field");
        if (fieldType.isAggregatable() == false) {
            return; // No field data available, so we ignore
        }
        SourceToParse source = source(this::writeField);
        ParsedDocument doc = mapperService.documentMapper().parse(source);

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            LeafReaderContext ctx = ir.leaves().get(0);

            ScriptDocValues<?> fieldData = fieldType.fielddataBuilder("test", () -> { throw new UnsupportedOperationException(); })
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
                .load(ctx)
                .getScriptValues();

            fieldData.setNextDocId(0);

            DocumentLeafReader reader = new DocumentLeafReader(doc.rootDoc(), Collections.emptyMap());
            ScriptDocValues<?> indexData = fieldType.fielddataBuilder("test", () -> { throw new UnsupportedOperationException(); })
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
                .load(reader.getContext())
                .getScriptValues();
            indexData.setNextDocId(0);

            // compare index and search time fielddata
            assertThat(fieldData, equalTo(indexData));
        });
    }

    protected boolean supportsStoredFields() {
        return true;
    }

    protected void minimalStoreMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
        b.field("store", true);
    }

    /**
     * Checks that loading stored fields for this field produces the same set of values
     * for query time scripts and index time scripts
     */
    public final void testIndexTimeStoredFieldsAccess() throws IOException {

        assumeTrue("Field type does not support stored fields", supportsStoredFields());
        MapperService mapperService = createMapperService(fieldMapping(this::minimalStoreMapping));
        assertParseMinimalWarnings();

        MappedFieldType fieldType = mapperService.fieldType("field");
        SourceToParse source = source(this::writeField);
        ParsedDocument doc = mapperService.documentMapper().parse(source);

        SearchLookup lookup = new SearchLookup(f -> fieldType, (f, s) -> { throw new UnsupportedOperationException(); });

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            LeafReaderContext ctx = ir.leaves().get(0);
            LeafStoredFieldsLookup storedFields = lookup.getLeafSearchLookup(ctx).fields();
            storedFields.setDocument(0);

            DocumentLeafReader reader = new DocumentLeafReader(doc.rootDoc(), Collections.emptyMap());

            LeafStoredFieldsLookup indexStoredFields = lookup.getLeafSearchLookup(reader.getContext()).fields();
            indexStoredFields.setDocument(0);

            // compare index and search time stored fields
            assertThat(storedFields.get("field").getValues(), equalTo(indexStoredFields.get("field").getValues()));
        });
    }

    public final void testNullInput() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        if (allowsNullValues()) {
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            assertThat(doc.docs().get(0).getFields("field").length, equalTo(0));
            assertThat(doc.docs().get(0).getFields("_field_names").length, equalTo(0));
        } else {
            expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.nullField("field"))));
        }

        assertWarnings(getWarningLevel(), getParseMinimalWarnings());
    }

    protected boolean allowsNullValues() {
        return true;
    }
}
