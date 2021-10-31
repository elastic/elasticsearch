/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.LongFieldScript.LeafFactory;
import org.elasticsearch.script.QueryableExpression;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldTypeRandomAproximateQueryTests extends MapperServiceTestCase {

    public void testAddToLong() throws IOException {
        withIndexedLong((indexed, searcher, context) -> {
            long added = randomInterestingLong();
            long result = indexed + added;
            logger.info("{} + {} = {}", indexed, added, result);
            MappedFieldType ft = build("add_long", Map.of("added", added));
            checkQueries(searcher, context, ft, result);
        });
    }

    public void testMultiplyWithLong() throws IOException {
        withIndexedLong((indexed, searcher, context) -> {
            long multiplied = randomInterestingLong();
            long result = indexed * multiplied;
            logger.info("{} * {} = {}", indexed, multiplied, result);
            MappedFieldType ft = build("multiply_long", Map.of("multiplied", multiplied));
            checkQueries(searcher, context, ft, result);
        });
    }

    public void testDivideByLong() throws IOException {
        withIndexedLong((indexed, searcher, context) -> {
            long divisor = randomValueOtherThan(0L, this::randomInterestingLong);
            long result = indexed / divisor;
            logger.info("{} / {} = {}", indexed, divisor, result);
            MappedFieldType ft = build("divide_long", Map.of("divisor", divisor));
            checkQueries(searcher, context, ft, result);
        });
    }

    private long randomInterestingLong() {
        switch (between(0, 3)) {
            case 0:
                return 0;
            case 1:
                return randomBoolean() ? 1 : -1;
            case 2:
                return randomInt();
            case 3:
                return randomLong();
            default:
                throw new IllegalArgumentException("Unsupported case");
        }
    }

    @FunctionalInterface
    interface WithIndexedLong {
        void accept(long indexed, IndexSearcher searcher, SearchExecutionContext context) throws IOException;
    }

    private void withIndexedLong(WithIndexedLong callback) throws IOException {
        long indexed = randomLong();
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", indexed), new LongPoint("foo", indexed)));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext context = mockContext(new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG));
                callback.accept(indexed, searcher, context);
            }
        }
    }

    private void checkQueries(IndexSearcher searcher, SearchExecutionContext context, MappedFieldType ft, long result) throws IOException {
        assertCount(searcher, ft.termQuery(result, context), 1);
        assertCount(searcher, ft.termQuery(randomValueOtherThan(result, ESTestCase::randomLong), context), 0);

        assertCount(searcher, ft.rangeQuery(result - 1, result + 1, true, true, ShapeRelation.INTERSECTS, null, null, context), 1);
        assertCount(searcher, ft.rangeQuery(result + 1, result + 3, true, true, ShapeRelation.INTERSECTS, null, null, context), 0);
        assertCount(searcher, ft.rangeQuery(result - 3, result - 1, true, true, ShapeRelation.INTERSECTS, null, null, context), 0);
    }

    private void assertCount(IndexSearcher searcher, Query query, int count) throws IOException {
        assertThat("count for " + query, searcher.count(query), equalTo(count));
    }

    private static SearchExecutionContext mockContext(MappedFieldType sourceField) {
        assertTrue(sourceField.isSearchable());
        assertTrue(sourceField.hasDocValues());
        return AbstractScriptFieldTypeTestCase.mockContext(true, sourceField);
    }

    private static LongScriptFieldType build(String code, Map<String, Object> params) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new LongScriptFieldType("f", factory(script), script, true, emptyMap());
    }

    private static LongFieldScript.Factory factory(Script script) {
        switch (script.getIdOrCode()) {
            case "add_long":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo + (long) params.get("added"));
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpression emitExpression(Function<String, QueryableExpression> lookup, Map<String, Object> params) {
                        return lookup.apply("foo").add(QueryableExpression.constant((long) params.get("added")));
                    }
                };
            case "multiply_long":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo * (long) params.get("multiplied"));
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpression emitExpression(Function<String, QueryableExpression> lookup, Map<String, Object> params) {
                        return lookup.apply("foo").multiply(QueryableExpression.constant((long) params.get("multiplied")));
                    }
                };
            case "divide_long":
                return new LongFieldScript.Factory() {
                    @Override
                    public LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup lookup) {
                        return (ctx) -> new LongFieldScript(fieldName, params, lookup, ctx) {
                            @Override
                            public void execute() {
                                for (long foo : (ScriptDocValues.Longs) getDoc().get("foo")) {
                                    emit(foo / (long) params.get("divisor"));
                                }
                            }
                        };
                    }

                    @Override
                    public QueryableExpression emitExpression(Function<String, QueryableExpression> lookup, Map<String, Object> params) {
                        return lookup.apply("foo").divide(QueryableExpression.constant((long) params.get("divisor")));
                    }
                };
            default:
                throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        }
    }
}
