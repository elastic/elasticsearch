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
import org.elasticsearch.index.query.functionscore.GaussDecayFunctionBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.highlight.CustomHighlighter;
import org.elasticsearch.search.highlight.FastVectorHighlighter;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.highlight.PostingsHighlighter;
import org.elasticsearch.search.suggest.CustomSuggester;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;
import org.elasticsearch.search.suggest.term.TermSuggester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class SearchModuleTests extends ModuleTestCase {

    public void testDoubleRegister() {
        SearchPlugin registersDupeHighlighter = new SearchPlugin() {
            @Override
            public Map<String, Highlighter> getHighlighters() {
                return singletonMap("plain", new PlainHighlighter());
            }
        };
        expectThrows(IllegalArgumentException.class,
                () -> new SearchModule(Settings.EMPTY, false, singletonList(registersDupeHighlighter)));

        SearchPlugin registersDupeSuggester = new SearchPlugin() {
            @Override
            public Map<String,org.elasticsearch.search.suggest.Suggester<?>> getSuggesters() {
                return singletonMap("term", TermSuggester.INSTANCE);
            }
        };
        expectThrows(IllegalArgumentException.class,
                () -> new SearchModule(Settings.EMPTY, false, singletonList(registersDupeSuggester)));

        SearchPlugin registersDupeScoreFunction = new SearchPlugin() {
            @Override
            public List<ScoreFunctionSpec<?>> getScoreFunctions() {
                return singletonList(new ScoreFunctionSpec<>(GaussDecayFunctionBuilder.NAME, GaussDecayFunctionBuilder::new,
                        GaussDecayFunctionBuilder.PARSER));
            }
        };
        expectThrows(IllegalArgumentException.class,
                () -> new SearchModule(Settings.EMPTY, false, singletonList(registersDupeScoreFunction)));

        SearchPlugin registersDupeSignificanceHeuristic = new SearchPlugin() {
            @Override
            public List<SearchExtensionSpec<SignificanceHeuristic, SignificanceHeuristicParser>> getSignificanceHeuristics() {
                return singletonList(new SearchExtensionSpec<>(ChiSquare.NAME, ChiSquare::new, ChiSquare.PARSER));
            }
        };
        expectThrows(IllegalArgumentException.class, () -> new SearchModule(Settings.EMPTY, false,
                singletonList(registersDupeSignificanceHeuristic)));

        SearchPlugin registersDupeMovAvgModel = new SearchPlugin() {
            @Override
            public List<SearchExtensionSpec<MovAvgModel, MovAvgModel.AbstractModelParser>> getMovingAverageModels() {
                return singletonList(new SearchExtensionSpec<>(SimpleModel.NAME, SimpleModel::new, SimpleModel.PARSER));
            }
        };
        expectThrows(IllegalArgumentException.class, () -> new SearchModule(Settings.EMPTY, false,
                singletonList(registersDupeMovAvgModel)));

        SearchPlugin registersDupeFetchSubPhase = new SearchPlugin() {
            @Override
            public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
                return singletonList(new ExplainFetchSubPhase());
            }
        };
        expectThrows(IllegalArgumentException.class, () -> new SearchModule(Settings.EMPTY, false,
                singletonList(registersDupeFetchSubPhase)));

        SearchPlugin registersDupeFetchQuery = new SearchPlugin() {
            public List<SearchPlugin.QuerySpec<?>> getQueries() {
                return singletonList(new QuerySpec<>(TermQueryBuilder.NAME, TermQueryBuilder::new, TermQueryBuilder::fromXContent));
            }
        };
        expectThrows(IllegalArgumentException.class, () -> new SearchModule(Settings.EMPTY, false,
                singletonList(registersDupeFetchQuery)));
    }

    public void testRegisterSuggester() {
        SearchModule module = new SearchModule(Settings.EMPTY, false, singletonList(new SearchPlugin() {
            @Override
            public Map<String, Suggester<?>> getSuggesters() {
                return singletonMap("custom", CustomSuggester.INSTANCE);
            }
        }));
        assertSame(TermSuggester.INSTANCE, module.getSuggesters().getSuggester("term"));
        assertSame(PhraseSuggester.INSTANCE, module.getSuggesters().getSuggester("phrase"));
        assertSame(CompletionSuggester.INSTANCE, module.getSuggesters().getSuggester("completion"));
        assertSame(CustomSuggester.INSTANCE, module.getSuggesters().getSuggester("custom"));
    }

    public void testRegisterHighlighter() {
        CustomHighlighter customHighlighter = new CustomHighlighter();
        SearchModule module = new SearchModule(Settings.EMPTY, false, singletonList(new SearchPlugin() {
            @Override
            public Map<String, Highlighter> getHighlighters() {
                return singletonMap("custom", customHighlighter);
            }
        }));

        Map<String, Highlighter> highlighters = module.getHighlighters();
        assertEquals(FastVectorHighlighter.class, highlighters.get("fvh").getClass());
        assertEquals(PlainHighlighter.class, highlighters.get("plain").getClass());
        assertEquals(PostingsHighlighter.class, highlighters.get("postings").getClass());
        assertSame(highlighters.get("custom"), customHighlighter);
    }

    public void testRegisteredQueries() throws IOException {
        SearchModule module = new SearchModule(Settings.EMPTY, false, emptyList());
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
