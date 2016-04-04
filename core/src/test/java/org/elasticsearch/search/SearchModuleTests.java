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

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.TermQueryParser;
import org.elasticsearch.search.highlight.CustomHighlighter;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.suggest.CustomSuggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        module.registerSuggester("custom", CustomSuggester.PROTOTYPE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> module.registerSuggester("custom", CustomSuggester.PROTOTYPE));
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
        module.registerQueryParser(TermQueryParser::new);
        try {
            module.buildQueryParserRegistry();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("already registered for name [term] while trying to register [org.elasticsearch.index."));
        }
    }

    public void testRegisteredQueries() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        Map<String, QueryParser<?>> queryParserMap = module.buildQueryParserRegistry().queryParsers();
        assertThat(queryParserMap.size(), equalTo(84));
        assertThat(queryParserMap.keySet(), containsInAnyOrder(
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
                "fuzzy_match",
                "geoBbox",
                "geoBoundingBox",
                "geoDistance",
                "geoDistanceRange",
                "geoPolygon",
                "geoShape",
                "geo_bbox",
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
                "in",
                "indices",
                "match",
                "matchAll",
                "matchFuzzy",
                "matchNone",
                "matchPhrase",
                "matchPhrasePrefix",
                "match_all",
                "match_fuzzy",
                "match_none",
                "match_phrase",
                "match_phrase_prefix",
                "mlt",
                "moreLikeThis",
                "more_like_this",
                "multiMatch",
                "multi_match",
                "nested",
                "parent_id",
                "percolator",
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
                "wrapper"));
    }

    static class FakeQueryParser implements QueryParser {
        @Override
        public String[] names() {
            return new String[] {"fake-query-parser"};
        }

        @Override
        public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
            return null;
        }

        @Override
        public QueryBuilder getBuilderPrototype() {
            return null;
        }
    }

}
