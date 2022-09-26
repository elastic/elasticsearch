/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractScriptFieldTypeTestCase extends MapperServiceTestCase {

    private static final ToXContent.Params INCLUDE_DEFAULTS = new ToXContent.MapParams(Map.of("include_defaults", "true"));

    protected abstract MappedFieldType simpleMappedFieldType();

    protected abstract MappedFieldType loopFieldType();

    protected abstract String typeName();

    public final void testMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(runtimeFieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
    }

    public final void testMeta() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(b -> {
            minimalMapping(b);
            b.field("meta", Collections.singletonMap("foo", "bar"));
        });
        MapperService mapperService = createMapperService(mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = runtimeFieldMapping(this::minimalMapping);
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = runtimeFieldMapping(b -> {
            minimalMapping(b);
            b.field("meta", Collections.singletonMap("baz", "quux"));
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public final void testMinimalMappingToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(runtimeFieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, INCLUDE_DEFAULTS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
    }

    public void testCopyToIsNotSupported() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(b -> {
            minimalMapping(b);
            b.field("copy_to", "target");
        });
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(exception.getMessage(), containsString("unknown parameter [copy_to] on runtime field"));
    }

    public void testMultiFieldsIsNotSupported() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(b -> {
            minimalMapping(b);
            b.startObject("fields").startObject("test").field("type", "keyword").endObject().endObject();
        });
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(exception.getMessage(), containsString("unknown parameter [fields] on runtime field"));
    }

    public void testStoredScriptsAreNotSupported() throws Exception {
        XContentBuilder mapping = runtimeFieldMapping(b -> {
            b.field("type", typeName());
            b.startObject("script").field("id", "test").endObject();
        });
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: stored scripts are not supported for runtime field [field]", exception.getMessage());
    }

    public void testFieldCaps() throws Exception {
        MapperService scriptIndexMapping = createMapperService(runtimeFieldMapping(this::minimalMapping));
        MapperService concreteIndexMapping;
        {
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("field")
                .field("type", typeName())
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            concreteIndexMapping = createMapperService(mapping);
        }
        MappedFieldType scriptFieldType = scriptIndexMapping.fieldType("field");
        MappedFieldType concreteIndexType = concreteIndexMapping.fieldType("field");
        assertEquals(concreteIndexType.familyTypeName(), scriptFieldType.familyTypeName());
        assertEquals(concreteIndexType.isSearchable(), scriptFieldType.isSearchable());
        assertEquals(concreteIndexType.isAggregatable(), scriptFieldType.isAggregatable());
    }

    @SuppressWarnings("unused")
    public abstract void testDocValues() throws IOException;

    @SuppressWarnings("unused")
    public abstract void testSort() throws IOException;

    @SuppressWarnings("unused")
    public abstract void testUsedInScript() throws IOException;

    @SuppressWarnings("unused")
    public abstract void testExistsQuery() throws IOException;

    @SuppressWarnings("unused")
    public abstract void testRangeQuery() throws IOException;

    protected abstract Query randomRangeQuery(MappedFieldType ft, SearchExecutionContext ctx);

    @SuppressWarnings("unused")
    public abstract void testTermQuery() throws IOException;

    protected abstract Query randomTermQuery(MappedFieldType ft, SearchExecutionContext ctx);

    @SuppressWarnings("unused")
    public abstract void testTermsQuery() throws IOException;

    protected abstract Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx);

    protected static SearchExecutionContext mockContext() {
        return mockContext(true);
    }

    protected static FieldDataContext mockFielddataContext() {
        return new FieldDataContext("test", mockContext()::lookup, mockContext()::sourcePath, MappedFieldType.FielddataOperation.SCRIPT);
    }

    protected static SearchExecutionContext mockContext(boolean allowExpensiveQueries) {
        return mockContext(allowExpensiveQueries, null);
    }

    protected boolean supportsTermQueries() {
        return true;
    }

    protected boolean supportsRangeQueries() {
        return true;
    }

    protected static SearchExecutionContext mockContext(boolean allowExpensiveQueries, MappedFieldType mappedFieldType) {
        return mockContext(allowExpensiveQueries, mappedFieldType, new SourceLookup.ReaderSourceProvider());
    }

    protected static SearchExecutionContext mockContext(
        boolean allowExpensiveQueries,
        MappedFieldType mappedFieldType,
        SourceLookup.SourceProvider sourceProvider
    ) {
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        if (mappedFieldType != null) {
            when(context.getFieldType(anyString())).thenReturn(mappedFieldType);
        }
        when(context.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        SearchLookup lookup = new SearchLookup(
            context::getFieldType,
            (mft, lookupSupplier, fdo) -> mft.fielddataBuilder(new FieldDataContext("test", lookupSupplier, context::sourcePath, fdo))
                .build(null, null),
            sourceProvider
        );
        when(context.lookup()).thenReturn(lookup);
        when(context.getForField(any(), any())).then(args -> {
            MappedFieldType ft = args.getArgument(0);
            MappedFieldType.FielddataOperation fdo = args.getArgument(1);
            return ft.fielddataBuilder(new FieldDataContext("test", context::lookup, context::sourcePath, fdo))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        });
        return context;
    }

    public void testExistsQueryIsExpensive() {
        checkExpensiveQuery(MappedFieldType::existsQuery);
    }

    public void testExistsQueryInLoop() {
        checkLoop(MappedFieldType::existsQuery);
    }

    public void testRangeQueryWithShapeRelationIsError() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> simpleMappedFieldType().rangeQuery(1, 2, true, true, ShapeRelation.DISJOINT, null, null, null)
        );
        assertThat(e.getMessage(), equalTo("Runtime field [test] of type [" + typeName() + "] does not support DISJOINT ranges"));
    }

    public void testRangeQueryIsExpensive() {
        assumeTrue("Impl does not support range queries", supportsRangeQueries());
        checkExpensiveQuery(this::randomRangeQuery);
    }

    public void testRangeQueryInLoop() {
        assumeTrue("Impl does not support range queries", supportsRangeQueries());
        checkLoop(this::randomRangeQuery);
    }

    public void testTermQueryIsExpensive() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        checkExpensiveQuery(this::randomTermQuery);
    }

    public void testTermQueryInLoop() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        checkLoop(this::randomTermQuery);
    }

    public void testTermsQueryIsExpensive() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        checkExpensiveQuery(this::randomTermsQuery);
    }

    public void testTermsQueryInLoop() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        checkLoop(this::randomTermsQuery);
    }

    public void testPhraseQueryIsError() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        assertQueryOnlyOnText("phrase", () -> simpleMappedFieldType().phraseQuery(null, 1, false, null));
    }

    public void testPhrasePrefixQueryIsError() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        assertQueryOnlyOnText("phrase prefix", () -> simpleMappedFieldType().phrasePrefixQuery(null, 1, 1, null));
    }

    public void testMultiPhraseQueryIsError() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        assertQueryOnlyOnText("phrase", () -> simpleMappedFieldType().multiPhraseQuery(null, 1, false, null));
    }

    public void testSpanPrefixQueryIsError() {
        assumeTrue("Impl does not support term queries", supportsTermQueries());
        assertQueryOnlyOnText("span prefix", () -> simpleMappedFieldType().spanPrefixQuery(null, null, null));
    }

    public final void testCacheable() throws IOException {
        XContentBuilder mapping = runtimeMapping(b -> {
            b.startObject("field")
                .field("type", typeName())
                .startObject("script")
                .field("source", "dummy_source")
                .field("lang", "test")
                .endObject()
                .endObject()
                .startObject("field_source")
                .field("type", typeName())
                .startObject("script")
                .field("source", "deterministic_source")
                .field("lang", "test")
                .endObject()
                .endObject();
        });

        MapperService mapperService = createMapperService(mapping);

        {
            SearchExecutionContext c = createSearchExecutionContext(mapperService);
            c.getFieldType("field").existsQuery(c);
            assertFalse(c.isCacheable());
        }

        {
            SearchExecutionContext c = createSearchExecutionContext(mapperService);
            c.getFieldType("field_source").existsQuery(c);
            assertTrue(c.isCacheable());
        }
    }

    private void assertQueryOnlyOnText(String queryName, ThrowingRunnable buildQuery) {
        Exception e = expectThrows(IllegalArgumentException.class, buildQuery);
        assertThat(
            e.getMessage(),
            equalTo(
                "Can only use "
                    + queryName
                    + " queries on text fields - not on [test] which is a runtime field of type ["
                    + typeName()
                    + "]"
            )
        );
    }

    protected final String readSource(IndexReader reader, int docId) throws IOException {
        return reader.document(docId).getBinaryValue("_source").utf8ToString();
    }

    protected final void checkExpensiveQuery(BiConsumer<MappedFieldType, SearchExecutionContext> queryBuilder) {
        Exception e = expectThrows(ElasticsearchException.class, () -> queryBuilder.accept(simpleMappedFieldType(), mockContext(false)));
        assertThat(
            e.getMessage(),
            equalTo("queries cannot be executed against runtime fields while [search.allow_expensive_queries] is set to [false].")
        );
    }

    protected final void checkLoop(BiConsumer<MappedFieldType, SearchExecutionContext> queryBuilder) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> queryBuilder.accept(loopFieldType(), mockContext()));
        assertThat(e.getMessage(), equalTo("Cyclic dependency detected while resolving runtime fields: test -> test"));
    }

    protected final void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", typeName());
        b.startObject("script").field("source", "dummy_source").field("lang", "test").endObject();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        boolean deterministicSource = "deterministic_source".equals(script.getIdOrCode());
        if (context == BooleanFieldScript.CONTEXT) {
            return deterministicSource ? (T) BooleanFieldScript.PARSE_FROM_SOURCE : (T) BooleanFieldScriptTests.DUMMY;
        }
        if (context == DateFieldScript.CONTEXT) {
            return deterministicSource ? (T) DateFieldScript.PARSE_FROM_SOURCE : (T) DateFieldScriptTests.DUMMY;
        }
        if (context == DoubleFieldScript.CONTEXT) {
            return deterministicSource ? (T) DoubleFieldScript.PARSE_FROM_SOURCE : (T) DoubleFieldScriptTests.DUMMY;
        }
        if (context == IpFieldScript.CONTEXT) {
            return deterministicSource ? (T) IpFieldScript.PARSE_FROM_SOURCE : (T) IpFieldScriptTests.DUMMY;
        }
        if (context == LongFieldScript.CONTEXT) {
            return deterministicSource ? (T) LongFieldScript.PARSE_FROM_SOURCE : (T) LongFieldScriptTests.DUMMY;
        }
        if (context == StringFieldScript.CONTEXT) {
            return deterministicSource ? (T) StringFieldScript.PARSE_FROM_SOURCE : (T) StringFieldScriptTests.DUMMY;
        }
        if (context == GeoPointFieldScript.CONTEXT) {
            return deterministicSource ? (T) GeoPointFieldScript.PARSE_FROM_SOURCE : (T) GeoPointFieldScriptTests.DUMMY;
        }
        throw new IllegalArgumentException("Unsupported context: " + context);
    }
}
