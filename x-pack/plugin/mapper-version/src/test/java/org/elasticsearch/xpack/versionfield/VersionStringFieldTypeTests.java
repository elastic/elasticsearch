/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VersionStringFieldTypeTests extends FieldTypeTestCase {

    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/154068:
     * fuzzyQuery must accept both String and BytesRef values without ClassCastException,
     * and both must produce equivalent queries.
     */
    public void testFuzzyQueryWithStringAndBytesRef() {
        MappedFieldType ft = new VersionStringFieldMapper.Builder("field").build(MapperBuilderContext.root(false, false)).fieldType();
        // String value — the path that used to throw ClassCastException (from MatchQueryParser)
        FuzzyQuery fromString = (FuzzyQuery) ft.fuzzyQuery("2.1", Fuzziness.ONE, 0, 50, true, MOCK_CONTEXT, null);
        // BytesRef value — the pre-existing path
        FuzzyQuery fromBytesRef = (FuzzyQuery) ft.fuzzyQuery(new BytesRef("2.1"), Fuzziness.ONE, 0, 50, true, MOCK_CONTEXT, null);

        assertEquals("field", fromString.getTerm().field());
        assertEquals(fromString.getTerm(), fromBytesRef.getTerm());
        assertEquals(fromString.getMaxEdits(), fromBytesRef.getMaxEdits());
    }

    /** With a request breaker in the context, the pattern is built step by step on it, as it is for keyword fields. */
    public void testRegexpQueryIsChargedToTheBreaker() {
        MappedFieldType ft = new VersionStringFieldMapper.Builder("field").build(MapperBuilderContext.root(false, false)).fieldType();
        // A mock context, because the field type reads only the expensive-queries setting and the breaker from it.
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.allowExpensiveQueries()).thenReturn(true);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        when(context.getCircuitBreaker()).thenReturn(breaker);
        expectThrows(CircuitBreakingException.class, () -> ft.regexpQuery("x?{500}{2}", RegExp.ALL, 0, 10_000, null, context));
        assertEquals(0L, breaker.getUsed());
        Query query = ft.regexpQuery("2\\.1.*", RegExp.ALL, 0, 10_000, null, context);
        assertEquals("/2\\.1.*/", query.toString("field"));
        assertEquals(0L, breaker.getUsed());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new VersionStringFieldMapper.Builder("field").build(MapperBuilderContext.root(false, false)).fieldType();
        assertEquals(List.of("value"), fetchSourceValue(mapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(mapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] doesn't support formats.", e.getMessage());
    }
}
