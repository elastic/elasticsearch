/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.BytesRefSortScript;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.StringSortScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.search.sort.NestedSortBuilderTests.createRandomNestedSort;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptSortBuilderTests extends AbstractSortTestCase<ScriptSortBuilder> {

    @Override
    protected ScriptSortBuilder createTestItem() {
        return randomScriptSortBuilder();
    }

    public static ScriptSortBuilder randomScriptSortBuilder() {
        ScriptSortType type = randomBoolean() ? ScriptSortType.NUMBER : ScriptSortType.STRING;
        ScriptSortBuilder builder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), type);
        if (randomBoolean()) {
            builder.order(randomFrom(SortOrder.values()));
        }
        if (randomBoolean()) {
            if (type == ScriptSortType.NUMBER) {
                builder.sortMode(randomValueOtherThan(builder.sortMode(), () -> randomFrom(SortMode.values())));
            } else {
                Set<SortMode> exceptThis = new HashSet<>();
                exceptThis.add(SortMode.SUM);
                exceptThis.add(SortMode.AVG);
                exceptThis.add(SortMode.MEDIAN);
                builder.sortMode(randomValueOtherThanMany(exceptThis::contains, () -> randomFrom(SortMode.values())));
            }
        }
        if (randomBoolean()) {
            builder.setNestedSort(createRandomNestedSort(3));
        }
        return builder;
    }

    @Override
    protected ScriptSortBuilder mutate(ScriptSortBuilder original) throws IOException {
        ScriptSortBuilder result;
        if (randomBoolean()) {
            // change one of the constructor args, copy the rest over
            Script script = original.script();
            ScriptSortType type = original.type();
            if (randomBoolean()) {
                result = new ScriptSortBuilder(mockScript(script.getIdOrCode() + "_suffix"), type);
            } else {
                result = new ScriptSortBuilder(script, type.equals(ScriptSortType.NUMBER) ? ScriptSortType.STRING : ScriptSortType.NUMBER);
            }
            result.order(original.order());
            if (original.sortMode() != null && result.type() == ScriptSortType.NUMBER) {
                result.sortMode(original.sortMode());
            }
            result.setNestedSort(original.getNestedSort());
            return result;
        }
        result = new ScriptSortBuilder(original);
        switch (randomIntBetween(0, 2)) {
            case 0:
                if (original.order() == SortOrder.ASC) {
                    result.order(SortOrder.DESC);
                } else {
                    result.order(SortOrder.ASC);
                }
                break;
            case 1:
                if (original.type() == ScriptSortType.NUMBER) {
                    result.sortMode(randomValueOtherThan(result.sortMode(), () -> randomFrom(SortMode.values())));
                } else {
                    // script sort type String only allows MIN and MAX, so we only switch
                    if (original.sortMode() == SortMode.MIN) {
                        result.sortMode(SortMode.MAX);
                    } else {
                        result.sortMode(SortMode.MIN);
                    }
                }
                break;
            case 2:
                result.setNestedSort(
                    randomValueOtherThan(original.getNestedSort(), () -> NestedSortBuilderTests.createRandomNestedSort(3))
                );
                break;
        }
        return result;
    }

    @Override
    protected void sortFieldAssertions(ScriptSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertEquals(SortField.Type.CUSTOM, sortField.getType());
        assertEquals(builder.order() == SortOrder.ASC ? false : true, sortField.getReverse());
    }

    public void testScriptSortType() {
        // we rely on these ordinals in serialization, so changing them breaks bwc.
        assertEquals(0, ScriptSortType.STRING.ordinal());
        assertEquals(1, ScriptSortType.NUMBER.ordinal());
        assertEquals(2, ScriptSortType.VERSION.ordinal());

        assertEquals("string", ScriptSortType.STRING.toString());
        assertEquals("number", ScriptSortType.NUMBER.toString());
        assertEquals("version", ScriptSortType.VERSION.toString());

        assertEquals(ScriptSortType.STRING, ScriptSortType.fromString("string"));
        assertEquals(ScriptSortType.STRING, ScriptSortType.fromString("String"));
        assertEquals(ScriptSortType.STRING, ScriptSortType.fromString("STRING"));
        assertEquals(ScriptSortType.NUMBER, ScriptSortType.fromString("number"));
        assertEquals(ScriptSortType.NUMBER, ScriptSortType.fromString("Number"));
        assertEquals(ScriptSortType.NUMBER, ScriptSortType.fromString("NUMBER"));
        assertEquals(ScriptSortType.VERSION, ScriptSortType.fromString("version"));
        assertEquals(ScriptSortType.VERSION, ScriptSortType.fromString("Version"));
        assertEquals(ScriptSortType.VERSION, ScriptSortType.fromString("VERSION"));
    }

    public void testScriptSortTypeNull() {
        Exception e = expectThrows(NullPointerException.class, () -> ScriptSortType.fromString(null));
        assertEquals("input string is null", e.getMessage());
    }

    public void testScriptSortTypeIllegalArgument() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortType.fromString("xyz"));
        assertEquals("Unknown ScriptSortType [xyz]", e.getMessage());
    }

    public void testParseJson() throws IOException {
        String scriptSort = """
            {
              "_script": {
                "type": "number",
                "script": {
                  "source": "doc['field_name'].value * factor",
                  "params": {
                    "factor": 1.1
                  }
                },
                "mode": "max",
                "order": "asc"
              }
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            ScriptSortBuilder builder = ScriptSortBuilder.fromXContent(parser, null);
            assertEquals("doc['field_name'].value * factor", builder.script().getIdOrCode());
            assertEquals(Script.DEFAULT_SCRIPT_LANG, builder.script().getLang());
            assertEquals(1.1, builder.script().getParams().get("factor"));
            assertEquals(ScriptType.INLINE, builder.script().getType());
            assertEquals(ScriptSortType.NUMBER, builder.type());
            assertEquals(SortOrder.ASC, builder.order());
            assertEquals(SortMode.MAX, builder.sortMode());
            assertNull(builder.getNestedSort());
        }
    }

    public void testParseJson_simple() throws IOException {
        String scriptSort = """
            {
              "_script": {
                "type": "number",
                "script": "doc['field_name'].value",
                "mode": "max",
                "order": "asc"
              }
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            ScriptSortBuilder builder = ScriptSortBuilder.fromXContent(parser, null);
            assertEquals("doc['field_name'].value", builder.script().getIdOrCode());
            assertEquals(Script.DEFAULT_SCRIPT_LANG, builder.script().getLang());
            assertEquals(builder.script().getParams(), Collections.emptyMap());
            assertEquals(ScriptType.INLINE, builder.script().getType());
            assertEquals(ScriptSortType.NUMBER, builder.type());
            assertEquals(SortOrder.ASC, builder.order());
            assertEquals(SortMode.MAX, builder.sortMode());
            assertNull(builder.getNestedSort());
        }
    }

    public void testParseBadFieldNameExceptions() throws IOException {
        String scriptSort = """
            {"_script" : {"bad_field" : "number"} }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            XContentParseException e = expectThrows(XContentParseException.class, () -> ScriptSortBuilder.fromXContent(parser, null));
            assertEquals("[1:15] [_script] unknown field [bad_field]", e.getMessage());
        }
    }

    public void testParseBadFieldNameExceptionsOnStartObject() throws IOException {
        String scriptSort = """
            {"_script" : {"bad_field" : { "order" : "asc" } } }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            XContentParseException e = expectThrows(XContentParseException.class, () -> ScriptSortBuilder.fromXContent(parser, null));
            assertEquals("[1:15] [_script] unknown field [bad_field]", e.getMessage());
        }
    }

    public void testParseUnexpectedToken() throws IOException {
        String scriptSort = """
            {"_script" : {"script" : [ "order" : "asc" ] } }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            Exception e = expectThrows(XContentParseException.class, () -> ScriptSortBuilder.fromXContent(parser, null));
            assertThat(e.getMessage(), containsString("[_script] script doesn't support values of type: START_ARRAY"));
        }
    }

    /**
     * script sort of type {@link ScriptSortType} does not work with {@link SortMode#AVG}, {@link SortMode#MEDIAN} or {@link SortMode#SUM}
     */
    public void testBadSortMode() throws IOException {
        ScriptSortBuilder builder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.STRING);
        String sortMode = randomFrom(new String[] { "avg", "median", "sum" });
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.sortMode(SortMode.fromString(sortMode)));
        assertEquals("script sort of type [string] doesn't support mode [" + sortMode + "]", e.getMessage());
    }

    /**
     * Test that the sort builder mode gets transferred correctly to the SortField
     */
    public void testMultiValueMode() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();
        for (SortMode mode : SortMode.values()) {
            ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
            sortBuilder.sortMode(mode);
            SortField sortField = sortBuilder.build(searchExecutionContext).field();
            assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
            XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
            assertEquals(MultiValueMode.fromString(mode.toString()), comparatorSource.sortMode());
        }

        // check that without mode set, order ASC sets mode to MIN, DESC to MAX
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        sortBuilder.order(SortOrder.ASC);
        SortField sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MIN, comparatorSource.sortMode());

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        sortBuilder.order(SortOrder.DESC);
        sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MAX, comparatorSource.sortMode());
    }

    /**
     * Test that the correct comparator sort is returned, based on the script type
     */
    public void testBuildCorrectComparatorType() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.STRING);
        SortField sortField = sortBuilder.build(createMockSearchExecutionContext()).field();
        assertThat(sortField.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        sortField = sortBuilder.build(createMockSearchExecutionContext()).field();
        assertThat(sortField.getComparatorSource(), instanceOf(DoubleValuesComparatorSource.class));

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.VERSION);
        sortField = sortBuilder.build(createMockSearchExecutionContext()).field();
        assertThat(sortField.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));
    }

    /**
     * Test that the sort builder nested object gets created in the SortField
     */
    public void testBuildNested() throws IOException {
        SearchExecutionContext searchExecutionContext = createMockSearchExecutionContext();

        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER).setNestedSort(
            new NestedSortBuilder("path").setFilter(QueryBuilders.matchAllQuery())
        );
        SortField sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        Nested nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(Queries.ALL_DOCS_INSTANCE, nested.getInnerQuery());

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER).setNestedSort(
            new NestedSortBuilder("path")
        );
        sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new TermQuery(new Term(NestedPathFieldMapper.NAME, "path")), nested.getInnerQuery());

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER).setNestedSort(
            new NestedSortBuilder("path").setFilter(QueryBuilders.matchAllQuery())
        );
        sortField = sortBuilder.build(searchExecutionContext).field();
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(Queries.ALL_DOCS_INSTANCE, nested.getInnerQuery());
    }

    /**
     * Test the nested Filter gets rewritten
     */
    public void testNestedRewrites() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript("something"), ScriptSortType.STRING);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doSearchRewrite(SearchExecutionContext context) {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedSort(new NestedSortBuilder("path").setFilter(rangeQuery));
        ScriptSortBuilder rewritten = sortBuilder.rewrite(createMockSearchExecutionContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

    /**
     * Test the nested sort gets rewritten
     */
    public void testNestedSortRewrites() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript("something"), ScriptSortType.STRING);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doSearchRewrite(SearchExecutionContext context) {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedSort(new NestedSortBuilder("path").setFilter(rangeQuery));
        ScriptSortBuilder rewritten = sortBuilder.rewrite(createMockSearchExecutionContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

    public void testCancellationCheckWiredForNumberSortScript() throws IOException {
        AtomicReference<NumberSortScript> capturedScript = new AtomicReference<>();
        NumberSortScript.LeafFactory leafFactory = new NumberSortScript.LeafFactory() {
            @Override
            public NumberSortScript newInstance(DocReader reader) throws IOException {
                NumberSortScript script = new NumberSortScript(Map.of(), null, reader) {
                    @Override
                    public double execute() {
                        return 0;
                    }
                };
                capturedScript.set(script);
                return script;
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
        NumberSortScript.Factory factory = (params, searchLookup) -> leafFactory;

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                SearchExecutionContext context = mock(SearchExecutionContext.class);
                doReturn(factory).when(context).compile(any(Script.class), same(NumberSortScript.CONTEXT));
                when(context.lookup()).thenReturn(new SearchLookup(null, null, (ctx, doc) -> null));
                when(context.searcher()).thenReturn(contextSearcher);
                SortField sortField = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER).build(context).field();
                XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
                comparatorSource.newComparator("_script", 1, Pruning.NONE, false).getLeafComparator(reader.leaves().get(0));
                assertNotNull(capturedScript.get()._getCancellationCheck());
            }
        }
    }

    public void testCancellationCheckWiredForStringSortScript() throws IOException {
        AtomicReference<StringSortScript> capturedScript = new AtomicReference<>();
        StringSortScript.LeafFactory leafFactory = new StringSortScript.LeafFactory() {
            @Override
            public StringSortScript newInstance(DocReader reader) throws IOException {
                StringSortScript script = new StringSortScript(Map.of(), reader) {
                    @Override
                    public String execute() {
                        return "";
                    }
                };
                capturedScript.set(script);
                return script;
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
        StringSortScript.Factory factory = params -> leafFactory;

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                SearchExecutionContext context = mock(SearchExecutionContext.class);
                doReturn(factory).when(context).compile(any(Script.class), same(StringSortScript.CONTEXT));
                when(context.lookup()).thenReturn(new SearchLookup(null, null, (ctx, doc) -> null));
                when(context.searcher()).thenReturn(contextSearcher);
                SortField sortField = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.STRING).build(context).field();
                XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
                comparatorSource.newComparator("_script", 1, Pruning.NONE, false).getLeafComparator(reader.leaves().get(0));
                assertNotNull(capturedScript.get()._getCancellationCheck());
            }
        }
    }

    public void testCancellationCheckWiredForBytesRefSortScript() throws IOException {
        AtomicReference<BytesRefSortScript> capturedScript = new AtomicReference<>();
        BytesRefSortScript.LeafFactory leafFactory = new BytesRefSortScript.LeafFactory() {
            @Override
            public BytesRefSortScript newInstance(DocReader reader) throws IOException {
                BytesRefSortScript script = new BytesRefSortScript(Map.of(), reader) {
                    @Override
                    public Object execute() {
                        return null;
                    }
                };
                capturedScript.set(script);
                return script;
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
        BytesRefSortScript.Factory factory = params -> leafFactory;

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                SearchExecutionContext context = mock(SearchExecutionContext.class);
                doReturn(factory).when(context).compile(any(Script.class), same(BytesRefSortScript.CONTEXT));
                when(context.lookup()).thenReturn(new SearchLookup(null, null, (ctx, doc) -> null));
                when(context.searcher()).thenReturn(contextSearcher);
                SortField sortField = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.VERSION).build(context).field();
                XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
                comparatorSource.newComparator("_script", 1, Pruning.NONE, false).getLeafComparator(reader.leaves().get(0));
                assertNotNull(capturedScript.get()._getCancellationCheck());
            }
        }
    }

    @Override
    protected ScriptSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return ScriptSortBuilder.fromXContent(parser, fieldName);
    }
}
