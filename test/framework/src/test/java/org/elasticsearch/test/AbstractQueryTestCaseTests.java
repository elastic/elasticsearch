/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.elasticsearch.test.AbstractQueryTestCase.alterateQueries;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Various test for {@link org.elasticsearch.test.AbstractQueryTestCase}
 */
public class AbstractQueryTestCaseTests extends ESTestCase {

    private final String STANDARD_ERROR = "unknown field [newField]";

    public void testAlterateQueries() throws IOException {
        List<Tuple<String, String>> alterations = alterateQueries(singleton("{\"field\": \"value\"}"), null);
        assertAlterations(alterations, allOf(notNullValue(), hasEntry("{\"newField\":{\"field\":\"value\"}}", STANDARD_ERROR)));

        alterations = alterateQueries(singleton("""
            {"term":{"field": "value"}}"""), null);
        assertAlterations(alterations, allOf(hasEntry("""
            {"newField":{"term":{"field":"value"}}}""", STANDARD_ERROR), hasEntry("""
            {"term":{"newField":{"field":"value"}}}""", STANDARD_ERROR)));

        alterations = alterateQueries(singleton("""
            {"bool":{"must": [{"match":{"field":"value"}}]}}"""), null);
        assertAlterations(alterations, allOf(hasEntry("""
            {"newField":{"bool":{"must":[{"match":{"field":"value"}}]}}}""", STANDARD_ERROR), hasEntry("""
            {"bool":{"newField":{"must":[{"match":{"field":"value"}}]}}}""", STANDARD_ERROR), hasEntry("""
            {"bool":{"must":[{"newField":{"match":{"field":"value"}}}]}}""", STANDARD_ERROR), hasEntry("""
            {"bool":{"must":[{"match":{"newField":{"field":"value"}}}]}}""", STANDARD_ERROR)));

        alterations = alterateQueries(singleton("""
            {"function_score":{"query": {"term":{"foo": "bar"}}, "script_score": {"script":"a + 1", "params": {"a":0}}}}"""), null);
        assertAlterations(alterations, allOf(hasEntry("""
            {"newField":{"function_score":{"query":{"term":{"foo":"bar"}},\
            "script_score":{"script":"a + 1","params":{"a":0}}}}}""", STANDARD_ERROR), hasEntry("""
            {"function_score":{"newField":{"query":{"term":{"foo":"bar"}},\
            "script_score":{"script":"a + 1","params":{"a":0}}}}}""", STANDARD_ERROR), hasEntry("""
            {"function_score":{"query":{"newField":{"term":{"foo":"bar"}}},\
            "script_score":{"script":"a + 1","params":{"a":0}}}}""", STANDARD_ERROR), hasEntry("""
            {"function_score":{"query":{"term":{"newField":{"foo":"bar"}}},\
            "script_score":{"script":"a + 1","params":{"a":0}}}}""", STANDARD_ERROR), hasEntry("""
            {"function_score":{"query":{"term":{"foo":"bar"}},\
            "script_score":{"newField":{"script":"a + 1","params":{"a":0}}}}}""", STANDARD_ERROR), hasEntry("""
            {"function_score":{"query":{"term":{"foo":"bar"}},\
            "script_score":{"script":"a + 1","params":{"newField":{"a":0}}}}}""", STANDARD_ERROR)));
    }

    public void testAlterateQueriesWithArbitraryContent() throws IOException {
        Map<String, String> arbitraryContentHolders = new HashMap<>();
        arbitraryContentHolders.put("params", null); // no exception expected
        arbitraryContentHolders.put("doc", "my own error");
        Set<String> queries = Sets.newHashSet("""
            {"query":{"script":"test","params":{"foo":"bar"}}}""", """
            {"query":{"more_like_this":{"fields":["a","b"],"like":{"doc":{"c":"d"}}}}}""");

        List<Tuple<String, String>> alterations = alterateQueries(queries, arbitraryContentHolders);
        assertAlterations(alterations, allOf(hasEntry("""
            {"newField":{"query":{"script":"test","params":{"foo":"bar"}}}}""", STANDARD_ERROR), hasEntry("""
            {"query":{"newField":{"script":"test","params":{"foo":"bar"}}}}""", STANDARD_ERROR), hasEntry("""
            {"query":{"script":"test","params":{"newField":{"foo":"bar"}}}}""", null)));
        assertAlterations(alterations, allOf(hasEntry("""
            {"newField":{"query":{"more_like_this":{"fields":["a","b"],"like":{"doc":{"c":"d"}}}}}}""", STANDARD_ERROR), hasEntry("""
            {"query":{"newField":{"more_like_this":{"fields":["a","b"],"like":{"doc":{"c":"d"}}}}}}""", STANDARD_ERROR), hasEntry("""
            {"query":{"more_like_this":{"newField":{"fields":["a","b"],"like":{"doc":{"c":"d"}}}}}}""", STANDARD_ERROR), hasEntry("""
            {"query":{"more_like_this":{"fields":["a","b"],"like":{"newField":{"doc":{"c":"d"}}}}}}""", STANDARD_ERROR), hasEntry("""
            {"query":{"more_like_this":{"fields":["a","b"],"like":{"doc":{"newField":{"c":"d"}}}}}}""", "my own error")));
    }

    private static <K, V> void assertAlterations(List<Tuple<K, V>> alterations, Matcher<Map<K, V>> matcher) {
        Map<K, V> alterationsMap = new HashMap<>();
        alterations.forEach(t -> alterationsMap.put(t.v1(), t.v2()));
        assertThat(alterationsMap, matcher);
    }
}
