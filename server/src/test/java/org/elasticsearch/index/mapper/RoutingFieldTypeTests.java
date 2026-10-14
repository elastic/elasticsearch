/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.queries.XSortedSetDocValuesRangeQuery;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RoutingFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        Query expected = new TermQuery(new Term("_routing", new BytesRef("foo")));
        assertEquals(expected, RoutingFieldMapper.FIELD_TYPE.termQuery("foo", MOCK_CONTEXT));
    }

    public void testTermQueryDocValues() {
        BytesRef value = new BytesRef("foo");
        Query query = RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.termQuery(value.utf8ToString(), MOCK_CONTEXT);
        assertEquals("XSortedSetDocValuesRangeQuery", query.getClass().getSimpleName());
        assertEquals("[" + value + " TO " + value + "]", query.toString("_routing"));
    }

    public void testTermsQuery() {
        List<BytesRef> terms = List.of(new BytesRef("foo"), new BytesRef("bar"));
        Query expected = new TermInSetQuery("_routing", terms);
        TermInSetQuery actual = (TermInSetQuery) RoutingFieldMapper.FIELD_TYPE.termsQuery(List.of("foo", "bar"), MOCK_CONTEXT);
        assertEquals(expected, actual);
        assertEquals(MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, actual.getRewriteMethod());
    }

    public void testTermsQueryDocValues() {
        TermInSetQuery expected = new TermInSetQuery("_routing", List.of(new BytesRef("foo"), new BytesRef("bar")));
        TermInSetQuery actual = (TermInSetQuery) RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.termsQuery(List.of("foo", "bar"), MOCK_CONTEXT);
        assertEquals(expected, actual);
        assertEquals(MultiTermQuery.DOC_VALUES_REWRITE, actual.getRewriteMethod());
    }

    /**
     * On a slice-enabled index term queries redirect to the hash-prefixed {@code _slice_key}, the primary index sort.
     */
    public void testTermQuerySliceEnabled() {
        Query query = RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.termQuery("foo", sliceEnabledMockContext());
        assertEquals(sliceKeyExactQuery("foo"), query);
    }

    public void testTermsQuerySliceEnabled() {
        Query query = RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.termsQuery(List.of("foo", "bar"), sliceEnabledMockContext());
        Query expected = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(sliceKeyExactQuery("foo"), BooleanClause.Occur.SHOULD)
                .add(sliceKeyExactQuery("bar"), BooleanClause.Occur.SHOULD)
                .build()
        );
        assertEquals(expected, query);

        // a single distinct value collapses to the exact query
        query = RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.termsQuery(List.of("foo", "foo"), sliceEnabledMockContext());
        assertEquals(sliceKeyExactQuery("foo"), query);
    }

    /**
     * Past the max clause count, where one exact query per slice would throw {@code TooManyClauses},
     * the terms query falls back to a set query on the encoded keys.
     */
    public void testTermsQuerySliceEnabledManySlices() {
        List<String> slices = IntStream.rangeClosed(0, IndexSearcher.getMaxClauseCount()).mapToObj(i -> "slice-" + i).toList();
        Query query = RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.termsQuery(slices, sliceEnabledMockContext());
        Query expected = SortedDocValuesField.newSlowSetQuery(
            SliceIndexing.SLICE_KEY_FIELD_NAME,
            slices.stream().map(SliceIndexing::encodeSliceKey).toList()
        );
        assertEquals(expected, query);
    }

    private static Query sliceKeyExactQuery(String slice) {
        return XSortedSetDocValuesRangeQuery.newSlowExactQuery(SliceIndexing.SLICE_KEY_FIELD_NAME, SliceIndexing.encodeSliceKey(slice));
    }

    private static SearchExecutionContext sliceEnabledMockContext() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.allowExpensiveQueries()).thenReturn(true);
        Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        when(context.getIndexSettings()).thenReturn(IndexSettingsModule.newIndexSettings("test", settings));
        return context;
    }

    public void testPrefixQuery() {
        MappedFieldType ft = RoutingFieldMapper.FIELD_TYPE;

        Query expected = new PrefixQuery(new Term("_routing", new BytesRef("foo*")));
        assertEquals(expected, ft.prefixQuery("foo*", null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.prefixQuery("foo*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = RoutingFieldMapper.FIELD_TYPE;

        Query expected = new RegexpQuery(new Term("_routing", new BytesRef("foo?")));
        assertEquals(expected, ft.regexpQuery("foo?", 0, 0, 10, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo?", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testWildcardQuery() {
        MappedFieldType ft = RoutingFieldMapper.FIELD_TYPE;

        Query expected = new WildcardQuery(new Term("_routing", new BytesRef("foo*")));
        assertEquals(expected, ft.wildcardQuery("foo*", null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.wildcardQuery("valu*", null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = RoutingFieldMapper.FIELD_TYPE;

        Query expected = new TermRangeQuery("_routing", new BytesRef("foo"), new BytesRef("qux"), true, false);
        assertEquals(expected, ft.rangeQuery("foo", "qux", true, false, null, null, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("foo", "qux", true, false, null, null, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRangeQueryDocValues() {
        Query query = RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.rangeQuery("foo", "qux", true, false, null, null, null, MOCK_CONTEXT);
        assertEquals("XSortedSetDocValuesRangeQuery", query.getClass().getSimpleName());
        assertEquals("[" + new BytesRef("foo") + " TO " + new BytesRef("qux") + "}", query.toString("_routing"));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.rangeQuery(
                "foo",
                "qux",
                true,
                false,
                null,
                null,
                null,
                MOCK_CONTEXT_DISALLOW_EXPENSIVE
            )
        );
        assertEquals(
            "Cannot search on field [_routing] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testPrefixQueryDocValues() {
        Query expected = new PrefixQuery(new Term("_routing", new BytesRef("foo")), MultiTermQuery.DOC_VALUES_REWRITE);
        assertEquals(expected, RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.prefixQuery("foo", null, false, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.prefixQuery("foo", null, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Cannot search on field [_routing] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testWildcardQueryDocValues() {
        Query expected = new WildcardQuery(
            new Term("_routing", new BytesRef("foo*")),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            MultiTermQuery.DOC_VALUES_REWRITE
        );
        assertEquals(expected, RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.wildcardQuery("foo*", null, false, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.wildcardQuery("foo*", null, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "Cannot search on field [_routing] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testFuzzyQueryDocValues() {
        Query expected = new FuzzyQuery(
            new Term("_routing", new BytesRef("foo")),
            Fuzziness.ONE.asDistance("foo"),
            0,
            50,
            true,
            MultiTermQuery.DOC_VALUES_REWRITE
        );
        assertEquals(expected, RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.fuzzyQuery("foo", Fuzziness.ONE, 0, 50, true, MOCK_CONTEXT, null));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> RoutingFieldMapper.DOC_VALUES_FIELD_TYPE.fuzzyQuery(
                "foo",
                Fuzziness.ONE,
                0,
                50,
                true,
                MOCK_CONTEXT_DISALLOW_EXPENSIVE,
                null
            )
        );
        assertEquals(
            "Cannot search on field [_routing] since it is not indexed and 'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }
}
