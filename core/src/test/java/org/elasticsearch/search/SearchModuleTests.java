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
import org.elasticsearch.search.highlight.FastVectorHighlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.highlight.PostingsHighlighter;
import org.elasticsearch.search.suggest.CustomSuggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class SearchModuleTests extends ModuleTestCase {

   public void testDoubleRegister() {
       SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
       try {
           module.registerHighlighter("fvh", new PlainHighlighter());
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
        CustomHighlighter customHighlighter = new CustomHighlighter();
        module.registerHighlighter("custom",  customHighlighter);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> module.registerHighlighter("custom", new CustomHighlighter()));
        assertEquals("Can't register the same [highlighter] more than once for [custom]", exception.getMessage());

        exception = expectThrows(IllegalArgumentException.class,
            () -> module.registerHighlighter("custom", null));
        assertEquals("Can't register null highlighter for key: [custom]", exception.getMessage());
        Highlighters highlighters = module.getHighlighters();
        assertEquals(highlighters.get("fvh").getClass(), FastVectorHighlighter.class);
        assertEquals(highlighters.get("plain").getClass(), PlainHighlighter.class);
        assertEquals(highlighters.get("postings").getClass(), PostingsHighlighter.class);
        assertSame(highlighters.get("custom"), customHighlighter);
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
            indicesQueriesRegistry.lookup(queryName, ParseFieldMatcher.EMPTY, dummyParser.getTokenLocation());
        }

        for (String queryName : NON_DEPRECATED_QUERIES) {
            QueryParser<?> queryParser = indicesQueriesRegistry.lookup(queryName, ParseFieldMatcher.STRICT, dummyParser.getTokenLocation());
            assertThat(queryParser, notNullValue());
        }
        for (String queryName : DEPRECATED_QUERIES) {
            try {
                indicesQueriesRegistry.lookup(queryName, ParseFieldMatcher.STRICT, dummyParser.getTokenLocation());
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
            "constant_score",
            "dis_max",
            "exists",
            "field_masking_span",
            "function_score",
            "fuzzy",
            "geo_bounding_box",
            "geo_distance",
            "geo_distance_range",
            "geo_polygon",
            "geo_shape",
            "geohash_cell",
            "has_child",
            "has_parent",
            "ids",
            "indices",
            "match",
            "match_all",
            "match_none",
            "match_phrase",
            "match_phrase_prefix",
            "more_like_this",
            "multi_match",
            "nested",
            "parent_id",
            "prefix",
            "query_string",
            "range",
            "regexp",
            "script",
            "simple_query_string",
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
            "fuzzy_match",
            "geo_bbox",
            "in",
            "match_fuzzy",
            "mlt"
    };
}
