/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.SharedAutomaton;
import org.elasticsearch.common.lucene.search.SharedAutomatonQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KeyedFlattenedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.KeyedArrayOrderInlineNullAutomatonQuery;
import org.elasticsearch.lucene.queries.KeyedArrayOrderInlineNullTermQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesAutomatonQuery;
import org.elasticsearch.lucene.queries.ScanningBinaryDocValuesTermQuery;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.SEPARATE_COUNT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyedFlattenedFieldTypeTests extends FieldTypeTestCase {

    private static final Mapper.IgnoreAbove IGNORE_ABOVE = new Mapper.IgnoreAbove(null, IndexMode.STANDARD);

    private static KeyedFlattenedFieldType createFieldType() {
        return new KeyedFlattenedFieldType(
            "field",
            IndexType.terms(true, true),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            true,
            false,
            false,
            null,
            IndexVersion.current(),
            false
        );
    }

    public void testIndexedValueForSearch() {
        KeyedFlattenedFieldType ft = createFieldType();

        BytesRef keywordValue = ft.indexedValueForSearch("value");
        assertEquals(new BytesRef("key\0value"), keywordValue);

        BytesRef doubleValue = ft.indexedValueForSearch(2.718);
        assertEquals(new BytesRef("key\0" + "2.718"), doubleValue);

        BytesRef booleanValue = ft.indexedValueForSearch(true);
        assertEquals(new BytesRef("key\0true"), booleanValue);
    }

    public void testTermQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new TermQuery(new Term(ft.name(), "key\0value"));
        assertEquals(expected, ft.termQuery("value", null));

        // case-insensitive on the value only
        Query caseInsensitive = ft.termQueryCaseInsensitive("value", null);
        assertTrue(accepts(caseInsensitive, "key\0VaLuE"));
        assertFalse(accepts(caseInsensitive, "KEY\0value"));
        assertFalse(accepts(caseInsensitive, "key\0values"));

        KeyedFlattenedFieldType unsearchable = new KeyedFlattenedFieldType(
            "field",
            IndexType.NONE,
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            true,
            false,
            false,
            null,
            IndexVersion.current(),
            false
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("field", null));
        assertEquals("Cannot search on field [" + ft.name() + "] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new TermInSetQuery(ft.name(), List.of(new BytesRef("key\0value1"), new BytesRef("key\0value2")));

        List<String> terms = new ArrayList<>();
        terms.add("value1");
        terms.add("value2");
        Query actual = ft.termsQuery(terms, null);

        assertEquals(expected, actual);
    }

    public void testTermQueryWithBinaryDocValuesOnly() {
        KeyedFlattenedFieldType ft = new KeyedFlattenedFieldType(
            "field",
            IndexType.docValuesOnly(),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            true,
            false,
            false,
            null,
            IndexVersion.current(),
            false
        );

        Query expected = new ScanningBinaryDocValuesTermQuery(ft.name(), new BytesRef("key\0value"), SEPARATE_COUNT);
        assertEquals(expected, ft.termQuery("value", null));

        assertThat(ft.termQueryCaseInsensitive("value", null), instanceOf(ScanningBinaryDocValuesAutomatonQuery.class));
        assertThat(ft.prefixQuery("val", null, randomBoolean(), MOCK_CONTEXT), instanceOf(ScanningBinaryDocValuesAutomatonQuery.class));
    }

    public void testTermQueryWithArrayOrderBinaryDocValues() {
        KeyedFlattenedFieldType ft = new KeyedFlattenedFieldType(
            "field",
            IndexType.docValuesOnly(),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            true,
            true,
            false,
            null,
            IndexVersion.current(),
            false
        );

        Query query = ft.termQuery("value", null);
        // The array-order path uses KeyedArrayOrderInlineNullTermQuery, a distinct class from
        // ScanningBinaryDocValuesTermQuery, giving each path a distinct query-cache identity.
        assertNotEquals(new ScanningBinaryDocValuesTermQuery(ft.name(), new BytesRef("key\0value"), SEPARATE_COUNT), query);
        assertTrue(query instanceof KeyedArrayOrderInlineNullTermQuery);

        assertThat(ft.termQueryCaseInsensitive("value", null), instanceOf(KeyedArrayOrderInlineNullAutomatonQuery.class));
        assertThat(ft.prefixQuery("val", null, randomBoolean(), MOCK_CONTEXT), instanceOf(KeyedArrayOrderInlineNullAutomatonQuery.class));
    }

    public void testTermQueryWithSortedSetDocValuesOnly() {
        KeyedFlattenedFieldType ft = new KeyedFlattenedFieldType(
            "field",
            IndexType.docValuesOnly(),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            false,
            false,
            false,
            null,
            IndexVersion.current(),
            false
        );

        Query expected = SortedSetDocValuesField.newSlowExactQuery(ft.name(), new BytesRef("key\0value"));
        assertEquals(expected, ft.termQuery("value", null));

        // without terms, the case-insensitive query reads the doc values
        AutomatonQuery caseInsensitive = (AutomatonQuery) ft.termQueryCaseInsensitive("value", null);
        assertSame(MultiTermQuery.DOC_VALUES_REWRITE, caseInsensitive.getRewriteMethod());
        assertTrue(accepts(caseInsensitive, "key\0VALUE"));
        assertFalse(accepts(caseInsensitive, "KEY\0value"));

        // so does a prefix query, case-sensitive or not
        AutomatonQuery prefix = (AutomatonQuery) ft.prefixQuery("val", null, false, MOCK_CONTEXT);
        assertSame(MultiTermQuery.DOC_VALUES_REWRITE, prefix.getRewriteMethod());
        assertTrue(accepts(prefix, "key\0value"));
        assertFalse(accepts(prefix, "key\0VALUE"));
        assertFalse(accepts(prefix, "other\0value"));
        AutomatonQuery caseInsensitivePrefix = (AutomatonQuery) ft.prefixQuery("vAL", null, true, MOCK_CONTEXT);
        assertTrue(accepts(caseInsensitivePrefix, "key\0value"));
        assertFalse(accepts(caseInsensitivePrefix, "KEY\0value"));
    }

    public void testTermsQueryWithBinaryDocValuesOnly() {
        KeyedFlattenedFieldType ft = new KeyedFlattenedFieldType(
            "field",
            IndexType.docValuesOnly(),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            true,
            false,
            false,
            null,
            IndexVersion.current(),
            false
        );

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(
            new ScanningBinaryDocValuesTermQuery(ft.name(), new BytesRef("key\0value"), SEPARATE_COUNT),
            BooleanClause.Occur.SHOULD
        );
        Query expected = new ConstantScoreQuery(builder.build());
        assertEquals(expected, ft.termsQuery(List.of("value"), null));
    }

    public void testTermsQueryWithArrayOrderBinaryDocValues() {
        KeyedFlattenedFieldType ft = new KeyedFlattenedFieldType(
            "field",
            IndexType.docValuesOnly(),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            true,
            true,
            false,
            null,
            IndexVersion.current(),
            false
        );

        Query result = ft.termsQuery(List.of("v1", "v2"), null);
        // The result is a ConstantScoreQuery wrapping a BooleanQuery of per-term KeyedArrayOrderInlineNullTermQuery clauses.
        assertTrue(result instanceof ConstantScoreQuery);
        BooleanQuery bq = (BooleanQuery) ((ConstantScoreQuery) result).getQuery();
        assertEquals(2, bq.clauses().size());
        for (BooleanClause clause : bq.clauses()) {
            assertTrue(clause.query() instanceof KeyedArrayOrderInlineNullTermQuery);
        }
        // Each clause uses KeyedArrayOrderInlineNullTermQuery, which has a distinct class identity from
        // plain ScanningBinaryDocValuesTermQuery. The resulting query must not equal the non-array-order
        // equivalent so the two paths get separate query-cache entries.
        BooleanQuery.Builder nonArrayBuilder = new BooleanQuery.Builder();
        nonArrayBuilder.add(
            new ScanningBinaryDocValuesTermQuery(ft.name(), new BytesRef("key\0v1"), SEPARATE_COUNT),
            BooleanClause.Occur.SHOULD
        );
        nonArrayBuilder.add(
            new ScanningBinaryDocValuesTermQuery(ft.name(), new BytesRef("key\0v2"), SEPARATE_COUNT),
            BooleanClause.Occur.SHOULD
        );
        assertNotEquals(new ConstantScoreQuery(nonArrayBuilder.build()), result);
    }

    public void testTermsQueryWithSortedSetDocValuesOnly() {
        KeyedFlattenedFieldType ft = new KeyedFlattenedFieldType(
            "field",
            IndexType.docValuesOnly(),
            "key",
            false,
            Collections.emptyMap(),
            false,
            IGNORE_ABOVE,
            false,
            false,
            false,
            null,
            IndexVersion.current(),
            false
        );

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(SortedSetDocValuesField.newSlowExactQuery(ft.name(), new BytesRef("key\0value")), BooleanClause.Occur.SHOULD);
        Query expected = new ConstantScoreQuery(builder.build());
        assertEquals(expected, ft.termsQuery(List.of("value"), null));
    }

    public void testExistsQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new PrefixQuery(new Term(ft.name(), "key\0"));
        assertEquals(expected, ft.existsQuery(null));
    }

    public void testPrefixQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new PrefixQuery(new Term(ft.name(), "key\0val"));
        assertEquals(expected, ft.prefixQuery("val", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, false, MOCK_CONTEXT));

        // case-insensitive on the value only
        Query caseInsensitive = ft.prefixQuery("vAl", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, true, MOCK_CONTEXT);
        assertTrue(accepts(caseInsensitive, "key\0VALUE"));
        assertFalse(accepts(caseInsensitive, "KEY\0value"));
        assertFalse(accepts(caseInsensitive, "other\0value"));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.prefixQuery("val", randomBoolean() ? null : CONSTANT_SCORE_BLENDED_REWRITE, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );
    }

    public void testFuzzyQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        FuzzyQuery query = (FuzzyQuery) ft.fuzzyQuery("value", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT);
        assertEquals(new Term(ft.name(), "key\0value"), query.getTerm());
        assertEquals(2, query.getMaxEdits());
        // the key and the separator match exactly, then the prefix of the value
        assertEquals("key\0".length() + 1, query.getPrefixLength());

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.fuzzyQuery("value", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testRangeQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        TermRangeQuery expected = new TermRangeQuery(ft.name(), new BytesRef("key\0lower"), new BytesRef("key\0upper"), false, false);
        assertEquals(expected, ft.rangeQuery("lower", "upper", false, false, MOCK_CONTEXT));

        expected = new TermRangeQuery(ft.name(), new BytesRef("key\0lower"), new BytesRef("key\0upper"), true, true);
        assertEquals(expected, ft.rangeQuery("lower", "upper", true, true, MOCK_CONTEXT));
    }

    /**
     * An open lower bound is substituted with the {@code key\0} sentinel inclusive. That term is
     * the smallest one this key can produce (it is also the encoded form of value {@code ""}), so
     * the resulting Lucene range still starts at the first term in the key's slice. The upper
     * bound is passed through unchanged, so the inclusivity flag the caller supplied for the
     * upper side carries forward.
     */
    public void testRangeQueryWithOpenLowerBoundUsesKeyPrefixSentinel() {
        KeyedFlattenedFieldType ft = createFieldType();

        TermRangeQuery exclusiveUpper = new TermRangeQuery(ft.name(), new BytesRef("key\0"), new BytesRef("key\0upper"), true, false);
        assertEquals(exclusiveUpper, ft.rangeQuery(null, "upper", false, false, MOCK_CONTEXT));

        TermRangeQuery inclusiveUpper = new TermRangeQuery(ft.name(), new BytesRef("key\0"), new BytesRef("key\0upper"), true, true);
        assertEquals(inclusiveUpper, ft.rangeQuery(null, "upper", false, true, MOCK_CONTEXT));
    }

    /**
     * An open upper bound is substituted with the {@code key\1} sentinel exclusive. Byte
     * {@code 0x01} is the lowest byte strictly greater than the {@code \0} separator so it sits
     * past every {@code key\0<value>} encoding for this key, and strictly before the first term
     * of any sibling key. The lower bound is passed through unchanged so the caller's
     * inclusivity flag for the lower side carries forward.
     */
    public void testRangeQueryWithOpenUpperBoundUsesKeyPrefixSentinel() {
        KeyedFlattenedFieldType ft = createFieldType();

        TermRangeQuery exclusiveLower = new TermRangeQuery(
            ft.name(),
            new BytesRef("key\0lower"),
            new BytesRef("key" + (char) 0x01),
            false,
            false
        );
        assertEquals(exclusiveLower, ft.rangeQuery("lower", null, false, false, MOCK_CONTEXT));

        TermRangeQuery inclusiveLower = new TermRangeQuery(
            ft.name(),
            new BytesRef("key\0lower"),
            new BytesRef("key" + (char) 0x01),
            true,
            false
        );
        assertEquals(inclusiveLower, ft.rangeQuery("lower", null, true, false, MOCK_CONTEXT));
    }

    public void testSingleSidedRangeQueryChargesBreakerOnceAndMarksPreCharged() {
        KeyedFlattenedFieldType ft = createFieldType();
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.allowExpensiveQueries()).thenReturn(true);

        Query query = ft.rangeQuery("lower", null, true, false, context);

        assertTrue("single-sided keyed range must build a TermRangeQuery", query instanceof TermRangeQuery);
        long ramBytesUsed = ((TermRangeQuery) query).ramBytesUsed();
        verify(context).addCircuitBreakerMemory(ramBytesUsed, ChildMemoryCircuitBreaker.CATEGORY_RANGE + ":" + ft.name());
        verify(context).markQueryMemoryPreCharged(query);
    }

    /**
     * Both bounds open is the same set the dedicated {@code existsQuery} produces, so the keyed
     * mapper rejects it with a message pointing the caller at the right API. The earlier "must
     * include both" wording from before single-sided ranges was relaxed to "must specify at
     * least one" to make the new contract obvious.
     */
    public void testRangeQueryRejectsBothBoundsOpen() {
        KeyedFlattenedFieldType ft = createFieldType();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), MOCK_CONTEXT)
        );
        assertEquals("[range] queries on keyed [flattened] fields must specify at least one of the upper or lower bounds.", e.getMessage());
    }

    /**
     * The expensive-queries gate is checked on both the closed-range and the new single-sided
     * paths. The error text is the same shared {@code [text] or [keyword]} message because both
     * paths bottom out in a {@link TermRangeQuery}. Asserting it on the open-bound path here
     * guards against the new code branch silently bypassing the gate.
     */
    public void testRangeQueryRespectsExpensiveQueriesSetting() {
        KeyedFlattenedFieldType ft = createFieldType();

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("lower", "upper", false, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );

        ee = expectThrows(ElasticsearchException.class, () -> ft.rangeQuery(null, "upper", false, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );

        ee = expectThrows(ElasticsearchException.class, () -> ft.rangeQuery("lower", null, false, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query query = ft.regexpQuery("valu*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, MOCK_CONTEXT);
        assertTrue(accepts(query, "key\0val"));
        assertTrue(accepts(query, "key\0valuu"));
        assertFalse(accepts(query, "key\0value"));
        assertFalse(accepts(query, "other\0val"));

        // an alternation stays under the key
        Query alternation = ft.regexpQuery("a|b", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, MOCK_CONTEXT);
        assertTrue(accepts(alternation, "key\0a"));
        assertTrue(accepts(alternation, "key\0b"));
        assertFalse(accepts(alternation, "b"));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("valu*", RegExp.ALL, 0, 10, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testWildcardQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query query = ft.wildcardQuery("valu*", null, false, MOCK_CONTEXT);
        assertTrue(accepts(query, "key\0value"));
        assertFalse(accepts(query, "key\0VALUE"));
        assertFalse(accepts(query, "other\0value"));

        Query caseInsensitive = ft.wildcardQuery("VALU*", null, true, MOCK_CONTEXT);
        assertTrue(accepts(caseInsensitive, "key\0value"));
        assertFalse(accepts(caseInsensitive, "KEY\0value"));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.wildcardQuery("valu*", null, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    /** Whether the automaton of {@code query} accepts the term {@code term}. */
    private static boolean accepts(Query query, String term) {
        Automaton automaton = query instanceof SharedAutomatonQuery shared
            ? shared.getAutomaton()
            : ((AutomatonQuery) query).getAutomaton();
        return Operations.run(automaton, term);
    }

    /**
     * With a circuit breaker, a pattern on a key builds its automaton as a {@code keyword} field does: guarded by the breaker,
     * charged to it, and matching the same terms as without one.
     */
    public void testPatternQueriesChargeTheCircuitBreaker() {
        KeyedFlattenedFieldType ft = createFieldType();
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        SearchExecutionContext context = contextWithBreaker(breaker);

        Query wildcard = ft.wildcardQuery("valu*", null, false, context);
        assertThat(wildcard, instanceOf(SharedAutomatonQuery.class));
        assertTrue(accepts(wildcard, "key\0value"));
        assertFalse(accepts(wildcard, "other\0value"));

        Query regexp = ft.regexpQuery("a|b", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, context);
        assertThat(regexp, instanceOf(SharedAutomatonQuery.class));
        assertTrue(accepts(regexp, "key\0b"));
        assertFalse(accepts(regexp, "b"));
        assertThat(breaker.getUsed(), greaterThan(0L));
        // what the request releases when it ends
        breaker.addWithoutBreaking(-breaker.getUsed());

        CircuitBreaker tiny = newLimitedBreaker(ByteSizeValue.ofBytes(64));
        expectThrows(CircuitBreakingException.class, () -> ft.wildcardQuery("v*a*l*u*e", null, false, contextWithBreaker(tiny)));
        assertThat(tiny.getUsed(), equalTo(0L));
    }

    /** A context that charges {@code breaker} and builds every shared automaton when asked for it. */
    private static SearchExecutionContext contextWithBreaker(CircuitBreaker breaker) {
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.allowExpensiveQueries()).thenReturn(true);
        when(context.getCircuitBreaker()).thenReturn(breaker);
        when(context.computeAutomatonIfAbsent(any(), any())).thenAnswer(
            invocation -> invocation.<Supplier<SharedAutomaton>>getArgument(1).get()
        );
        doAnswer(invocation -> {
            breaker.addEstimateBytesAndMaybeBreak(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(context).addCircuitBreakerMemory(anyLong(), anyString());
        return context;
    }

    public void testFetchIsEmpty() throws IOException {
        Map<String, Object> sourceValue = Map.of("key", "value");
        KeyedFlattenedFieldType ft = createFieldType();

        assertEquals(List.of(), fetchSourceValue(ft, sourceValue));
        assertEquals(List.of(), fetchSourceValue(ft, null));
    }

    public void testFetchSourceValue() throws IOException {
        KeyedFlattenedFieldType ft = createFieldType();
        Map<String, Object> sourceValue = Map.of("key", "value");

        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.getIndexSettings()).thenReturn(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY));
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath("field.key")).thenReturn(Set.of("field.key"));

        ValueFetcher fetcher = ft.valueFetcher(searchExecutionContext, null);
        {
            Source source = Source.fromMap(Collections.singletonMap("field", sourceValue), randomFrom(XContentType.values()));
            assertEquals(List.of("value"), fetcher.fetchValues(source, -1, new ArrayList<>()));
        }
        {
            Source source = Source.fromMap(Collections.singletonMap("field", null), randomFrom(XContentType.values()));
            assertEquals(List.of(), fetcher.fetchValues(source, -1, new ArrayList<>()));
        }

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ft.valueFetcher(searchExecutionContext, "format"));
        assertEquals("Field [field.key] of type [flattened] doesn't support formats.", e.getMessage());
    }
}
