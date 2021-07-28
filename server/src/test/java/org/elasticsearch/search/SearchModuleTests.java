/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.GaussDecayFunctionBuilder;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.ChiSquare;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalDerivative;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.ExplainPhase;
import org.elasticsearch.search.fetch.subphase.highlight.CustomHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.PlainHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.UnifiedHighlighter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SearchModuleTests extends ESTestCase {

    public void testDoubleRegister() {
        SearchPlugin registersDupeHighlighter = new SearchPlugin() {
            @Override
            public Map<String, Highlighter> getHighlighters() {
                return singletonMap("plain", new PlainHighlighter());
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeHighlighter));

        SearchPlugin registersDupeSuggester = new SearchPlugin() {
            @Override
            public List<SearchPlugin.SuggesterSpec<?>> getSuggesters() {
                return singletonList(new SuggesterSpec<>(TermSuggestionBuilder.SUGGESTION_NAME,
                    TermSuggestionBuilder::new, TermSuggestionBuilder::fromXContent, TermSuggestion::new));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeSuggester));

        SearchPlugin registersDupeScoreFunction = new SearchPlugin() {
            @Override
            public List<ScoreFunctionSpec<?>> getScoreFunctions() {
                return singletonList(new ScoreFunctionSpec<>(GaussDecayFunctionBuilder.NAME, GaussDecayFunctionBuilder::new,
                        GaussDecayFunctionBuilder.PARSER));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeScoreFunction));

        SearchPlugin registersDupeSignificanceHeuristic = new SearchPlugin() {
            @Override
            public List<SignificanceHeuristicSpec<?>> getSignificanceHeuristics() {
                return singletonList(new SignificanceHeuristicSpec<>(ChiSquare.NAME, ChiSquare::new, ChiSquare.PARSER));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeSignificanceHeuristic));

        SearchPlugin registersDupeFetchSubPhase = new SearchPlugin() {
            @Override
            public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
                return singletonList(new ExplainPhase());
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeFetchSubPhase));

        SearchPlugin registersDupeQuery = new SearchPlugin() {
            @Override
            public List<SearchPlugin.QuerySpec<?>> getQueries() {
                return singletonList(new QuerySpec<>(TermQueryBuilder.NAME, TermQueryBuilder::new, TermQueryBuilder::fromXContent));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeQuery));

        SearchPlugin registersDupeAggregation = new SearchPlugin() {
            @Override
            public List<AggregationSpec> getAggregations() {
                return singletonList(new AggregationSpec(TermsAggregationBuilder.NAME, TermsAggregationBuilder::new,
                        TermsAggregationBuilder.PARSER));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeAggregation));

        SearchPlugin registersDupePipelineAggregation = new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(new PipelineAggregationSpec(
                        DerivativePipelineAggregationBuilder.NAME,
                        DerivativePipelineAggregationBuilder::new,
                        DerivativePipelineAggregationBuilder::parse)
                            .addResultReader(InternalDerivative::new));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupePipelineAggregation));

        SearchPlugin registersDupeRescorer = new SearchPlugin() {
            @Override
            public List<RescorerSpec<?>> getRescorers() {
                return singletonList(
                        new RescorerSpec<>(QueryRescorerBuilder.NAME, QueryRescorerBuilder::new, QueryRescorerBuilder::fromXContent));
            }
        };
        expectThrows(IllegalArgumentException.class, registryForPlugin(registersDupeRescorer));
    }

    private ThrowingRunnable registryForPlugin(SearchPlugin plugin) {
        return () -> new NamedXContentRegistry(new SearchModule(Settings.EMPTY, singletonList(plugin))
            .getNamedXContents());
    }

    public void testRegisterSuggester() {
        SearchModule module = new SearchModule(Settings.EMPTY, singletonList(new SearchPlugin() {
            @Override
            public List<SuggesterSpec<?>> getSuggesters() {
                return singletonList(
                    new SuggesterSpec<>(
                        TestSuggestionBuilder.SUGGESTION_NAME,
                        TestSuggestionBuilder::new,
                        TestSuggestionBuilder::fromXContent,
                        TestSuggestion::new));
            }
        }));

        assertEquals(1, module.getNamedXContents().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) &&
                    e.name.match("term", LoggingDeprecationHandler.INSTANCE)).count());
        assertEquals(1, module.getNamedXContents().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) &&
                    e.name.match("phrase", LoggingDeprecationHandler.INSTANCE)).count());
        assertEquals(1, module.getNamedXContents().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) &&
                    e.name.match("completion", LoggingDeprecationHandler.INSTANCE)).count());
        assertEquals(1, module.getNamedXContents().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) &&
                    e.name.match("test", LoggingDeprecationHandler.INSTANCE)).count());

        assertEquals(1, module.getNamedWriteables().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) && e.name.equals("term")).count());
        assertEquals(1, module.getNamedWriteables().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) && e.name.equals("phrase")).count());
        assertEquals(1, module.getNamedWriteables().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) && e.name.equals("completion")).count());
        assertEquals(1, module.getNamedWriteables().stream()
                .filter(e -> e.categoryClass.equals(SuggestionBuilder.class) && e.name.equals("test")).count());

        assertEquals(1, module.getNamedWriteables().stream()
            .filter(e -> e.categoryClass.equals(Suggestion.class) && e.name.equals("term")).count());
        assertEquals(1, module.getNamedWriteables().stream()
            .filter(e -> e.categoryClass.equals(Suggestion.class) && e.name.equals("phrase")).count());
        assertEquals(1, module.getNamedWriteables().stream()
            .filter(e -> e.categoryClass.equals(Suggestion.class) && e.name.equals("completion")).count());
        assertEquals(1, module.getNamedWriteables().stream()
            .filter(e -> e.categoryClass.equals(Suggestion.class) && e.name.equals("test")).count());
    }

    public void testRegisterHighlighter() {
        CustomHighlighter customHighlighter = new CustomHighlighter();
        SearchModule module = new SearchModule(Settings.EMPTY, singletonList(new SearchPlugin() {
            @Override
            public Map<String, Highlighter> getHighlighters() {
                return singletonMap("custom", customHighlighter);
            }
        }));

        Map<String, Highlighter> highlighters = module.getHighlighters();
        assertEquals(FastVectorHighlighter.class, highlighters.get("fvh").getClass());
        assertEquals(PlainHighlighter.class, highlighters.get("plain").getClass());
        assertEquals(UnifiedHighlighter.class, highlighters.get("unified").getClass());
        assertSame(highlighters.get("custom"), customHighlighter);
    }

    public void testRegisteredQueries() {
        List<String> allSupportedQueries = new ArrayList<>();
        Collections.addAll(allSupportedQueries, NON_DEPRECATED_QUERIES);
        Collections.addAll(allSupportedQueries, DEPRECATED_QUERIES);
        SearchModule module = new SearchModule(Settings.EMPTY, emptyList());

        Set<String> registeredNonDeprecated = module.getNamedXContents().stream()
                .filter(e -> e.categoryClass.equals(QueryBuilder.class))
                .filter(e -> e.name.getAllReplacedWith() == null)
                .map(e -> e.name.getPreferredName())
                .collect(toSet());
        Set<String> registeredAll = module.getNamedXContents().stream()
                .filter(e -> e.categoryClass.equals(QueryBuilder.class))
                .flatMap(e -> Arrays.stream(e.name.getAllNamesIncludedDeprecated()))
                .collect(toSet());

        assertThat(registeredNonDeprecated, containsInAnyOrder(NON_DEPRECATED_QUERIES));
        assertThat(registeredAll, containsInAnyOrder(allSupportedQueries.toArray(new String[0])));
    }

    public void testRegisterAggregation() {
        SearchModule module = new SearchModule(Settings.EMPTY, singletonList(new SearchPlugin() {
            @Override
            public List<AggregationSpec> getAggregations() {
                return singletonList(new AggregationSpec("test", TestAggregationBuilder::new, TestAggregationBuilder::fromXContent));
            }
        }));

        assertThat(
                module.getNamedXContents().stream()
                    .filter(entry -> entry.categoryClass.equals(BaseAggregationBuilder.class) &&
                        entry.name.match("test", LoggingDeprecationHandler.INSTANCE))
                    .collect(toList()),
                hasSize(1));
    }

    public void testRegisterPipelineAggregation() {
        SearchModule module = new SearchModule(Settings.EMPTY, singletonList(new SearchPlugin() {
            @Override
            public List<PipelineAggregationSpec> getPipelineAggregations() {
                return singletonList(new PipelineAggregationSpec("test",
                        TestPipelineAggregationBuilder::new, TestPipelineAggregationBuilder::fromXContent));
            }
        }));

        assertThat(
                module.getNamedXContents().stream()
                    .filter(entry -> entry.categoryClass.equals(BaseAggregationBuilder.class) &&
                        entry.name.match("test", LoggingDeprecationHandler.INSTANCE))
                    .collect(toList()),
                hasSize(1));
    }

    public void testRegisterRescorer() {
        SearchModule module = new SearchModule(Settings.EMPTY, singletonList(new SearchPlugin() {
            @Override
            public List<RescorerSpec<?>> getRescorers() {
                return singletonList(new RescorerSpec<>("test", TestRescorerBuilder::new, TestRescorerBuilder::fromXContent));
            }
        }));
        assertThat(
                module.getNamedXContents().stream()
                    .filter(entry -> entry.categoryClass.equals(RescorerBuilder.class) &&
                        entry.name.match("test", LoggingDeprecationHandler.INSTANCE))
                    .collect(toList()),
                hasSize(1));
    }

    public void testRegisterNullRequestCacheKeyDifferentiator() {
        final SearchModule module = new SearchModule(Settings.EMPTY, List.of());
        assertThat(module.getRequestCacheKeyDifferentiator(), nullValue());
    }

    public void testRegisterRequestCacheKeyDifferentiator() {
        final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> requestCacheKeyDifferentiator = (r, o) -> { };
        final SearchModule module = new SearchModule(Settings.EMPTY, List.of(new SearchPlugin() {
            @Override
            public CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
                return requestCacheKeyDifferentiator;
            }
        }));
        assertThat(module.getRequestCacheKeyDifferentiator(), equalTo(requestCacheKeyDifferentiator));
    }

    public void testCannotRegisterMultipleRequestCacheKeyDifferentiators() {
        final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator1 = (r, o) -> {};
        final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator2 = (r, o) -> {};
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> new SearchModule(Settings.EMPTY, List.of(new SearchPlugin() {
                @Override
                public CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
                    return differentiator1;
                }
            }, new SearchPlugin() {
                @Override
                public CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
                    return differentiator2;
                }
            })));
        assertThat(e.getMessage(), containsString("Cannot have more than one plugin providing a request cache key differentiator"));
    }

    private static final String[] NON_DEPRECATED_QUERIES = new String[] {
            "bool",
            "boosting",
            "constant_score",
            "combined_fields",
            "dis_max",
            "exists",
            "function_score",
            "fuzzy",
            "geo_bounding_box",
            "geo_distance",
            "geo_shape",
            "ids",
            "intervals",
            "match",
            "match_all",
            "match_bool_prefix",
            "match_none",
            "match_phrase",
            "match_phrase_prefix",
            "more_like_this",
            "multi_match",
            "nested",
            "prefix",
            "query_string",
            "range",
            "regexp",
            "script",
            "script_score",
            "simple_query_string",
            "span_containing",
            "span_field_masking",
            "span_first",
            "span_gap",
            "span_multi",
            "span_near",
            "span_not",
            "span_or",
            "span_term",
            "span_within",
            "term",
            "terms",
            "terms_set",
            "wildcard",
            "wrapper",
            "distance_feature"
    };

    //add here deprecated queries to make sure we log a deprecation warnings when they are used
    private static final String[] DEPRECATED_QUERIES = new String[] {"field_masking_span", "geo_polygon"};

    /**
     * Dummy test {@link AggregationBuilder} used to test registering aggregation builders.
     */
    private static class TestAggregationBuilder extends ValuesSourceAggregationBuilder<TestAggregationBuilder> {
        protected TestAggregationBuilder(TestAggregationBuilder clone,
                                         Builder factoriesBuilder, Map<String, Object> metadata) {
            super(clone, factoriesBuilder, metadata);
        }

        @Override
        protected ValuesSourceType defaultValueSourceType() {
            return CoreValuesSourceType.KEYWORD;
        }

        @Override
        protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
            return new TestAggregationBuilder(this, factoriesBuilder, metadata);
        }
        /**
         * Read from a stream.
         */
        protected TestAggregationBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getType() {
            return "test";
        }

        @Override
        protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
            return ValuesSourceRegistry.UNREGISTERED_KEY;
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }

        @Override
        protected ValuesSourceAggregatorFactory innerBuild(AggregationContext context,
                                                           ValuesSourceConfig config,
                                                           AggregatorFactory parent,
                                                           Builder subFactoriesBuilder) throws IOException {
            return null;
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        private static TestAggregationBuilder fromXContent(String name, XContentParser p) {
            return null;
        }
    }

    /**
     * Dummy test {@link PipelineAggregator} used to test registering aggregation builders.
     */
    private static class TestPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<TestPipelineAggregationBuilder> {
        /**
         * Read from a stream.
         */
        TestPipelineAggregationBuilder(StreamInput in) throws IOException {
            super(in, "test");
        }

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metadata) {
            return null;
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        private static TestPipelineAggregationBuilder fromXContent(String name, XContentParser p) {
            return null;
        }

        @Override
        protected void validate(ValidationContext context) {}
    }

    /**
     * Dummy test {@link PipelineAggregator} used to test registering aggregation builders.
     */
    private static class TestPipelineAggregator extends PipelineAggregator {
        TestPipelineAggregator() {
            super("test", new String[] {}, null);
        }

        @Override
        public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
            return null;
        }
    }

    private static class TestRescorerBuilder extends RescorerBuilder<TestRescorerBuilder> {
        public static TestRescorerBuilder fromXContent(XContentParser parser) {
            return null;
        }

        TestRescorerBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public RescorerBuilder<TestRescorerBuilder> rewrite(QueryRewriteContext ctx) throws IOException {
            return this;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        }

        @Override
        public RescoreContext innerBuildContext(int windowSize, SearchExecutionContext context) throws IOException {
            return null;
        }
    }

    private static class TestSuggester extends Suggester<SuggestionSearchContext.SuggestionContext> {

        @Override
        protected Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> innerExecute(
                String name,
                SuggestionSearchContext.SuggestionContext suggestion,
                IndexSearcher searcher,
                CharsRefBuilder spare) throws IOException {
            return null;
        }

        @Override
        protected Suggestion<? extends Entry<? extends Option>> emptySuggestion(String name, SuggestionContext suggestion,
                CharsRefBuilder spare) throws IOException {
            return null;
        }
    }

    private static class TestSuggestionBuilder extends SuggestionBuilder<TestSuggestionBuilder> {

        public static final String SUGGESTION_NAME = "test";

        TestSuggestionBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {}

        public static TestSuggestionBuilder fromXContent(XContentParser parser) {
            return null;
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }

        @Override
        protected SuggestionSearchContext.SuggestionContext build(SearchExecutionContext context) throws IOException {
            return null;
        }

        @Override
        protected boolean doEquals(TestSuggestionBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return "test";
        }
    }

    private static class TestSuggestion extends Suggestion {
        TestSuggestion(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected Entry newEntry(StreamInput in) throws IOException {
            return null;
        }

        @Override
        public String getWriteableName() {
            return "test";
        }
    }
}
