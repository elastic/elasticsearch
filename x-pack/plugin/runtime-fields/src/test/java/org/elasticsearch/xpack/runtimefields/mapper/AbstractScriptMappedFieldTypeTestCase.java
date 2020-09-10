/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class AbstractScriptMappedFieldTypeTestCase extends ESTestCase {
    protected abstract MappedFieldType simpleMappedFieldType() throws IOException;

    protected abstract MappedFieldType loopFieldType() throws IOException;

    protected abstract String runtimeType();

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

    protected abstract Query randomRangeQuery(MappedFieldType ft, QueryShardContext ctx);

    @SuppressWarnings("unused")
    public abstract void testTermQuery() throws IOException;

    protected abstract Query randomTermQuery(MappedFieldType ft, QueryShardContext ctx);

    @SuppressWarnings("unused")
    public abstract void testTermsQuery() throws IOException;

    protected abstract Query randomTermsQuery(MappedFieldType ft, QueryShardContext ctx);

    protected static QueryShardContext mockContext() {
        return mockContext(true);
    }

    protected static QueryShardContext mockContext(boolean allowExpensiveQueries) {
        return mockContext(allowExpensiveQueries, null);
    }

    protected static QueryShardContext mockContext(boolean allowExpensiveQueries, MappedFieldType mappedFieldType) {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(anyString())).thenReturn(mappedFieldType);
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getMapperService()).thenReturn(mapperService);
        if (mappedFieldType != null) {
            when(context.fieldMapper(anyString())).thenReturn(mappedFieldType);
            when(context.getSearchAnalyzer(any())).thenReturn(mappedFieldType.getTextSearchInfo().getSearchAnalyzer());
        }
        when(context.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        SearchLookup lookup = new SearchLookup(
            mapperService,
            (mft, lookupSupplier) -> mft.fielddataBuilder("test", lookupSupplier).build(null, null, mapperService)
        );
        when(context.lookup()).thenReturn(lookup);
        return context;
    }

    public void testExistsQueryIsExpensive() throws IOException {
        checkExpensiveQuery(MappedFieldType::existsQuery);
    }

    public void testExistsQueryInLoop() throws IOException {
        checkLoop(MappedFieldType::existsQuery);
    }

    public void testRangeQueryWithShapeRelationIsError() throws IOException {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> simpleMappedFieldType().rangeQuery(1, 2, true, true, ShapeRelation.DISJOINT, null, null, null)
        );
        assertThat(
            e.getMessage(),
            equalTo("Field [test] of type [runtime] with runtime type [" + runtimeType() + "] does not support DISJOINT ranges")
        );
    }

    public void testRangeQueryIsExpensive() throws IOException {
        checkExpensiveQuery(this::randomRangeQuery);
    }

    public void testRangeQueryInLoop() throws IOException {
        checkLoop(this::randomRangeQuery);
    }

    public void testTermQueryIsExpensive() throws IOException {
        checkExpensiveQuery(this::randomTermQuery);
    }

    public void testTermQueryInLoop() throws IOException {
        checkLoop(this::randomTermQuery);
    }

    public void testTermsQueryIsExpensive() throws IOException {
        checkExpensiveQuery(this::randomTermsQuery);
    }

    public void testTermsQueryInLoop() throws IOException {
        checkLoop(this::randomTermsQuery);
    }

    public void testPhraseQueryIsError() {
        assertQueryOnlyOnText("phrase", () -> simpleMappedFieldType().phraseQuery(null, 1, false));
    }

    public void testPhrasePrefixQueryIsError() {
        assertQueryOnlyOnText("phrase prefix", () -> simpleMappedFieldType().phrasePrefixQuery(null, 1, 1));
    }

    public void testMultiPhraseQueryIsError() {
        assertQueryOnlyOnText("phrase", () -> simpleMappedFieldType().multiPhraseQuery(null, 1, false));
    }

    public void testSpanPrefixQueryIsError() {
        assertQueryOnlyOnText("span prefix", () -> simpleMappedFieldType().spanPrefixQuery(null, null, null));
    }

    private void assertQueryOnlyOnText(String queryName, ThrowingRunnable buildQuery) {
        Exception e = expectThrows(IllegalArgumentException.class, buildQuery);
        assertThat(
            e.getMessage(),
            equalTo(
                "Can only use "
                    + queryName
                    + " queries on text fields - not on [test] which is of type [runtime] with runtime_type ["
                    + runtimeType()
                    + "]"
            )
        );
    }

    protected String readSource(IndexReader reader, int docId) throws IOException {
        return reader.document(docId).getBinaryValue("_source").utf8ToString();
    }

    protected final void checkExpensiveQuery(BiConsumer<MappedFieldType, QueryShardContext> queryBuilder) throws IOException {
        Exception e = expectThrows(ElasticsearchException.class, () -> queryBuilder.accept(simpleMappedFieldType(), mockContext(false)));
        assertThat(
            e.getMessage(),
            equalTo("queries cannot be executed against [runtime] fields while [search.allow_expensive_queries] is set to [false].")
        );
    }

    protected final void checkLoop(BiConsumer<MappedFieldType, QueryShardContext> queryBuilder) throws IOException {
        Exception e = expectThrows(IllegalArgumentException.class, () -> queryBuilder.accept(loopFieldType(), mockContext()));
        assertThat(e.getMessage(), equalTo("Cyclic dependency detected while resolving runtime fields: test -> test"));
    }
}
