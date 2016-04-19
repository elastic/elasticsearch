/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.highlight.CustomHighlighter;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.suggest.CustomSuggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class SearchModuleTests extends ModuleTestCase {

   public void testDoubleRegister() {
       SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
       try {
           module.registerHighlighter("fvh", PlainHighlighter.class);
       } catch (IllegalArgumentException e) {
           assertEquals(e.getMessage(), "Can't register the same [highlighter] more than once for [fvh]");
       }

       try {
           module.registerSuggester("term", PhraseSuggester.INSTANCE);
       } catch (IllegalArgumentException e) {
           assertEquals(e.getMessage(), "Can't register the same [suggester] more than once for [term]");
       }
   }

    public void testRegisterSuggester() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        module.registerSuggester("custom", CustomSuggester.INSTANCE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> module.registerSuggester("custom", CustomSuggester.INSTANCE));
        assertEquals("Can't register the same [suggester] more than once for [custom]", e.getMessage());
    }

    public void testRegisterHighlighter() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        module.registerHighlighter("custom", CustomHighlighter.class);
        try {
            module.registerHighlighter("custom", CustomHighlighter.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [highlighter] more than once for [custom]");
        }
        assertMapMultiBinding(module, Highlighter.class, CustomHighlighter.class);
    }

    public void testRegisterQueryParserDuplicate() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> module
                .registerQuery(TermQueryBuilder::new, TermQueryBuilder::fromXContent, TermQueryBuilder.QUERY_NAME_FIELD));
        assertThat(e.getMessage(), containsString("] already registered for [query][term] while trying to register [org.elasticsearch."));
    }

    public void testRegisteredQueries() throws IOException {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        List<String> allSupportedQueries = new ArrayList<>();
        Collections.addAll(allSupportedQueries, NON_DEPRECATED_QUERIES);
        Collections.addAll(allSupportedQueries, DEPRECATED_QUERIES);
        String[] supportedQueries = allSupportedQueries.toArray(new String[allSupportedQueries.size()]);
        assertThat(module.getQueryParserRegistry().getNames(), containsInAnyOrder(supportedQueries));

        IndicesQueriesRegistry indicesQueriesRegistry = module.getQueryParserRegistry();
        XContentParser dummyParser = XContentHelper.createParser(new BytesArray("{}"));
        for (String queryName : supportedQueries) {
            indicesQueriesRegistry.lookup(queryName, dummyParser, ParseFieldMatcher.EMPTY);
        }

        for (String queryName : NON_DEPRECATED_QUERIES) {
            QueryParser<?> queryParser = indicesQueriesRegistry.lookup(queryName, dummyParser, ParseFieldMatcher.STRICT);
            assertThat(queryParser, notNullValue());
        }
        for (String queryName : DEPRECATED_QUERIES) {
            try {
                indicesQueriesRegistry.lookup(queryName, dummyParser, ParseFieldMatcher.STRICT);
                fail("query is deprecated, getQueryParser should have failed in strict mode");
            } catch(IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("Deprecated field [" + queryName + "] used"));
            }
        }
    }

    private static final String[] NON_DEPRECATED_QUERIES = new String[] {
            "bool",
            "boosting",
            "common",
            "constantScore",
            "constant_score",
            "disMax",
            "dis_max",
            "exists",
            "fieldMaskingSpan",
            "field_masking_span",
            "functionScore",
            "function_score",
            "fuzzy",
            "geoBoundingBox",
            "geoDistance",
            "geoDistanceRange",
            "geoPolygon",
            "geoShape",
            "geo_bounding_box",
            "geo_distance",
            "geo_distance_range",
            "geo_polygon",
            "geo_shape",
            "geohashCell",
            "geohash_cell",
            "hasChild",
            "hasParent",
            "has_child",
            "has_parent",
            "ids",
            "indices",
            "match",
            "matchAll",
            "matchNone",
            "matchPhrase",
            "matchPhrasePrefix",
            "match_all",
            "match_none",
            "match_phrase",
            "match_phrase_prefix",
            "moreLikeThis",
            "more_like_this",
            "multiMatch",
            "multi_match",
            "nested",
            "parentId",
            "parent_id",
            "percolate",
            "prefix",
            "queryString",
            "query_string",
            "range",
            "regexp",
            "script",
            "simpleQueryString",
            "simple_query_string",
            "spanContaining",
            "spanFirst",
            "spanMulti",
            "spanNear",
            "spanNot",
            "spanOr",
            "spanTerm",
            "spanWithin",
            "span_containing",
            "span_first",
            "span_multi",
            "span_near",
            "span_not",
            "span_or",
            "span_term",
            "span_within",
            "template",
            "term",
            "terms",
            "type",
            "wildcard",
            "wrapper"
    };

    private static final String[] DEPRECATED_QUERIES = new String[] {
            "fuzzyMatch",
            "fuzzy_match",
            "geoBbox",
            "geo_bbox",
            "in",
            "matchFuzzy",
            "match_fuzzy",
            "mlt"
    };
}
