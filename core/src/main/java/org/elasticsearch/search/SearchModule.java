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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.CommonTermsQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.FieldMaskingSpanQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceRangeQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.GeohashCellQuery;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.IndicesQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.ParentIdQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SpanContainingQueryBuilder;
import org.elasticsearch.index.query.SpanFirstQueryBuilder;
import org.elasticsearch.index.query.SpanMultiTermQueryBuilder;
import org.elasticsearch.index.query.SpanNearQueryBuilder;
import org.elasticsearch.index.query.SpanNotQueryBuilder;
import org.elasticsearch.index.query.SpanOrQueryBuilder;
import org.elasticsearch.index.query.SpanTermQueryBuilder;
import org.elasticsearch.index.query.SpanWithinQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.TypeQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.index.query.functionscore.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.WeightBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.FetchPhaseConstructionContext;
import org.elasticsearch.plugins.SearchPlugin.QuerySpec;
import org.elasticsearch.plugins.SearchPlugin.ScoreFunctionSpec;
import org.elasticsearch.plugins.SearchPlugin.SearchExtensionSpec;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.children.InternalChildren;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridParser;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingParser;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.InternalBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeParser;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeParser;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.InternalGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeParser;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedSamplerParser;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsParser;
import org.elasticsearch.search.aggregations.bucket.significant.UnmappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ScriptHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgParser;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityParser;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsParser;
import org.elasticsearch.search.aggregations.metrics.geobounds.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidParser;
import org.elasticsearch.search.aggregations.metrics.geocentroid.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxParser;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.InternalScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsParser;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsParser;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumParser;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountParser;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.InternalPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.InternalStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.InternalExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.InternalDerivative;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.EwmaModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltLinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.LinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregator;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.docvalues.DocValueFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.parent.ParentFieldSubFetchPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.FastVectorHighlighter;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.highlight.PostingsHighlighter;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.phrase.Laplace;
import org.elasticsearch.search.suggest.phrase.LinearInterpolation;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;
import org.elasticsearch.search.suggest.phrase.SmoothingModel;
import org.elasticsearch.search.suggest.phrase.StupidBackoff;
import org.elasticsearch.search.suggest.term.TermSuggester;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Sets up things that can be done at search time like queries, aggregations, and suggesters.
 */
public class SearchModule extends AbstractModule {

    private final boolean transportClient;
    private final Map<String, Highlighter> highlighters;
    private final Map<String, Suggester<?>> suggesters;
    private final ParseFieldRegistry<ScoreFunctionParser<?>> scoreFunctionParserRegistry = new ParseFieldRegistry<>("score_function");
    private final IndicesQueriesRegistry queryParserRegistry = new IndicesQueriesRegistry();
    private final ParseFieldRegistry<Aggregator.Parser> aggregationParserRegistry = new ParseFieldRegistry<>("aggregation");
    private final ParseFieldRegistry<PipelineAggregator.Parser> pipelineAggregationParserRegistry = new ParseFieldRegistry<>(
            "pipline_aggregation");
    private final AggregatorParsers aggregatorParsers = new AggregatorParsers(aggregationParserRegistry, pipelineAggregationParserRegistry);
    private final ParseFieldRegistry<SignificanceHeuristicParser> significanceHeuristicParserRegistry = new ParseFieldRegistry<>(
            "significance_heuristic");
    private final ParseFieldRegistry<MovAvgModel.AbstractModelParser> movingAverageModelParserRegistry = new ParseFieldRegistry<>(
            "moving_avg_model");

    private final List<FetchSubPhase> fetchSubPhases = new ArrayList<>();

    private final Settings settings;
    private final List<Entry> namedWriteables = new ArrayList<>();
    public static final Setting<Integer> INDICES_MAX_CLAUSE_COUNT_SETTING = Setting.intSetting("indices.query.bool.max_clause_count",
        1024, 1, Integer.MAX_VALUE, Setting.Property.NodeScope);

    // pkg private so tests can mock
    Class<? extends SearchService> searchServiceImpl = SearchService.class;

    public SearchModule(Settings settings, boolean transportClient, List<SearchPlugin> plugins) {
        this.settings = settings;
        this.transportClient = transportClient;
        suggesters = setupSuggesters(plugins);
        highlighters = setupHighlighters(settings, plugins);
        registerScoreFunctions(plugins);
        registerQueryParsers(plugins);
        registerRescorers();
        registerSorts();
        registerValueFormats();
        registerSignificanceHeuristics(plugins);
        registerMovingAverageModels(plugins);
        registerBuiltinAggregations();
        registerFetchSubPhases(plugins);
        registerShapes();
    }

    public List<Entry> getNamedWriteables() {
        return namedWriteables;
    }

    public Suggesters getSuggesters() {
        return new Suggesters(suggesters);
    }

    public IndicesQueriesRegistry getQueryParserRegistry() {
        return queryParserRegistry;
    }

    /**
     * Returns the {@link Highlighter} registry
     */
    public Map<String, Highlighter> getHighlighters() {
        return highlighters;
    }

    /**
     * The registry of {@link SignificanceHeuristic}s.
     */
    public ParseFieldRegistry<SignificanceHeuristicParser> getSignificanceHeuristicParserRegistry() {
        return significanceHeuristicParserRegistry;
    }

    /**
     * The registry of {@link MovAvgModel}s.
     */
    public ParseFieldRegistry<MovAvgModel.AbstractModelParser> getMovingAverageMdelParserRegistry() {
        return movingAverageModelParserRegistry;
    }

    /**
     * Register an aggregation.
     */
    public void registerAggregation(AggregationSpec spec) {
        if (false == transportClient) {
            aggregationParserRegistry.register(spec.parser, spec.name);
        }
        namedWriteables.add(new Entry(AggregationBuilder.class, spec.name.getPreferredName(), spec.builderReader));
        for (Map.Entry<String, Writeable.Reader<? extends InternalAggregation>> t : spec.resultReaders.entrySet()) {
            String writeableName = t.getKey();
            Writeable.Reader<? extends InternalAggregation> internalReader = t.getValue();
            namedWriteables.add(new Entry(InternalAggregation.class, writeableName, internalReader));
        }
    }

    public static class AggregationSpec {
        private final Map<String, Writeable.Reader<? extends InternalAggregation>> resultReaders = new TreeMap<>();
        private final Writeable.Reader<? extends AggregationBuilder> builderReader;
        private final Aggregator.Parser parser;
        private final ParseField name;

        /**
         * Register an aggregation.
         *
         * @param builderReader reads the {@link AggregationBuilder} from a stream
         * @param parser reads the aggregation builder from XContent
         * @param name names by which the aggregation may be parsed. The first name is special because it is the name that the reader is
         *        registered under.
         */
        public AggregationSpec(Reader<? extends AggregationBuilder> builderReader, Aggregator.Parser parser,
                ParseField name) {
            this.builderReader = builderReader;
            this.parser = parser;
            this.name = name;
        }

        /**
         * Add a reader for the shard level results of the aggregation with {@linkplain #name}'s {@link ParseField#getPreferredName()} as
         * the {@link NamedWriteable#getWriteableName()}.
         */
        public AggregationSpec addResultReader(Writeable.Reader<? extends InternalAggregation> resultReader) {
            return addResultReader(name.getPreferredName(), resultReader);
        }

        /**
         * Add a reader for the shard level results of the aggregation.
         */
        public AggregationSpec addResultReader(String writeableName, Writeable.Reader<? extends InternalAggregation> resultReader) {
            resultReaders.put(writeableName, resultReader);
            return this;
        }
    }

    /**
     * Register a pipeline aggregation.
     */
    public void registerPipelineAggregation(PipelineAggregationSpec spec) {
        if (false == transportClient) {
            pipelineAggregationParserRegistry.register(spec.parser, spec.name);
        }
        namedWriteables.add(new Entry(PipelineAggregationBuilder.class, spec.name.getPreferredName(), spec.builderReader));
        namedWriteables.add(new Entry(PipelineAggregator.class, spec.name.getPreferredName(), spec.aggregatorReader));
        for (Map.Entry<String, Writeable.Reader<? extends InternalAggregation>> resultReader : spec.resultReaders.entrySet()) {
            namedWriteables.add(new Entry(InternalAggregation.class, resultReader.getKey(), resultReader.getValue()));
        }
    }

    public static class PipelineAggregationSpec {
        private final Map<String, Writeable.Reader<? extends InternalAggregation>> resultReaders = new TreeMap<>();
        private final Writeable.Reader<? extends PipelineAggregationBuilder> builderReader;
        private final Writeable.Reader<? extends PipelineAggregator> aggregatorReader;
        private final PipelineAggregator.Parser parser;
        private final ParseField name;

        /**
         * Register a pipeline aggregation.
         *
         * @param builderReader reads the {@link PipelineAggregationBuilder} from a stream
         * @param aggregatorReader reads the {@link PipelineAggregator} from a stream
         * @param parser reads the aggregation builder from XContent
         * @param name names by which the aggregation may be parsed. The first name is special because it is the name that the reader is
         *        registered under.
         */
        public PipelineAggregationSpec(Reader<? extends PipelineAggregationBuilder> builderReader,
                Writeable.Reader<? extends PipelineAggregator> aggregatorReader,
                PipelineAggregator.Parser parser, ParseField name) {
            this.builderReader = builderReader;
            this.aggregatorReader = aggregatorReader;
            this.parser = parser;
            this.name = name;
        }

        /**
         * Add a reader for the shard level results of the aggregation with {@linkplain #name}'s {@link ParseField#getPreferredName()} as
         * the {@link NamedWriteable#getWriteableName()}.
         */
        public PipelineAggregationSpec addResultReader(Writeable.Reader<? extends InternalAggregation> resultReader) {
            return addResultReader(name.getPreferredName(), resultReader);
        }

        /**
         * Add a reader for the shard level results of the aggregation.
         */
        public PipelineAggregationSpec addResultReader(String writeableName, Writeable.Reader<? extends InternalAggregation> resultReader) {
            resultReaders.put(writeableName, resultReader);
            return this;
        }
    }

    @Override
    protected void configure() {
        if (false == transportClient) {
            /*
             * Nothing is bound for transport client *but* SearchModule is still responsible for settings up the things like the
             * NamedWriteableRegistry.
             */
            bind(IndicesQueriesRegistry.class).toInstance(queryParserRegistry);
            bind(Suggesters.class).toInstance(getSuggesters());
            configureSearch();
            bind(AggregatorParsers.class).toInstance(aggregatorParsers);
        }
    }

    private void registerBuiltinAggregations() {
        registerAggregation(new AggregationSpec(AvgAggregationBuilder::new, new AvgParser(), AvgAggregationBuilder.AGGREGATION_NAME_FIELD)
                .addResultReader(InternalAvg::new));
        registerAggregation(new AggregationSpec(SumAggregationBuilder::new, new SumParser(), SumAggregationBuilder.AGGREGATION_NAME_FIELD)
                .addResultReader(InternalSum::new));
        registerAggregation(new AggregationSpec(MinAggregationBuilder::new, new MinParser(), MinAggregationBuilder.AGGREGATION_NAME_FIELD)
                .addResultReader(InternalMin::new));
        registerAggregation(new AggregationSpec(MaxAggregationBuilder::new, new MaxParser(), MaxAggregationBuilder.AGGREGATION_NAME_FIELD)
                .addResultReader(InternalMax::new));
        registerAggregation(new AggregationSpec(StatsAggregationBuilder::new, new StatsParser(),
                StatsAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalStats::new));
        registerAggregation(new AggregationSpec(ExtendedStatsAggregationBuilder::new, new ExtendedStatsParser(),
                ExtendedStatsAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalExtendedStats::new));
        registerAggregation(new AggregationSpec(ValueCountAggregationBuilder::new, new ValueCountParser(),
                ValueCountAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalValueCount::new));
        registerAggregation(new AggregationSpec(PercentilesAggregationBuilder::new, new PercentilesParser(),
                PercentilesAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(InternalTDigestPercentiles.NAME, InternalTDigestPercentiles::new)
                    .addResultReader(InternalHDRPercentiles.NAME, InternalHDRPercentiles::new));
        registerAggregation(new AggregationSpec(PercentileRanksAggregationBuilder::new, new PercentileRanksParser(),
                PercentileRanksAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(InternalTDigestPercentileRanks.NAME, InternalTDigestPercentileRanks::new)
                    .addResultReader(InternalHDRPercentileRanks.NAME, InternalHDRPercentileRanks::new));
        registerAggregation(new AggregationSpec(CardinalityAggregationBuilder::new, new CardinalityParser(),
                CardinalityAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalCardinality::new));
        registerAggregation(new AggregationSpec(GlobalAggregationBuilder::new, GlobalAggregationBuilder::parse,
                GlobalAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalGlobal::new));
        registerAggregation(
                new AggregationSpec(MissingAggregationBuilder::new, new MissingParser(), MissingAggregationBuilder.AGGREGATION_NAME_FIELD)
                        .addResultReader(InternalMissing::new));
        registerAggregation(new AggregationSpec(FilterAggregationBuilder::new, FilterAggregationBuilder::parse,
                FilterAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalFilter::new));
        registerAggregation(new AggregationSpec(FiltersAggregationBuilder::new, FiltersAggregationBuilder::parse,
                FiltersAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalFilters::new));
        registerAggregation(new AggregationSpec(SamplerAggregationBuilder::new, SamplerAggregationBuilder::parse,
                SamplerAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalSampler.NAME, InternalSampler::new)
                        .addResultReader(UnmappedSampler.NAME, UnmappedSampler::new));
        registerAggregation(new AggregationSpec(DiversifiedAggregationBuilder::new, new DiversifiedSamplerParser(),
                DiversifiedAggregationBuilder.AGGREGATION_NAME_FIELD));
        registerAggregation(
                new AggregationSpec(TermsAggregationBuilder::new, new TermsParser(), TermsAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(StringTerms.NAME, StringTerms::new)
                    .addResultReader(UnmappedTerms.NAME, UnmappedTerms::new)
                    .addResultReader(LongTerms.NAME, LongTerms::new)
                    .addResultReader(DoubleTerms.NAME, DoubleTerms::new));
        registerAggregation(new AggregationSpec(SignificantTermsAggregationBuilder::new,
                new SignificantTermsParser(significanceHeuristicParserRegistry, queryParserRegistry),
                SignificantTermsAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(SignificantStringTerms.NAME, SignificantStringTerms::new)
                    .addResultReader(SignificantLongTerms.NAME, SignificantLongTerms::new)
                    .addResultReader(UnmappedSignificantTerms.NAME, UnmappedSignificantTerms::new));
        registerAggregation(new AggregationSpec(RangeAggregationBuilder::new, new RangeParser(),
                RangeAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalRange::new));
        registerAggregation(new AggregationSpec(DateRangeAggregationBuilder::new, new DateRangeParser(),
                DateRangeAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalDateRange::new));
        registerAggregation(
                new AggregationSpec(IpRangeAggregationBuilder::new, new IpRangeParser(), IpRangeAggregationBuilder.AGGREGATION_NAME_FIELD)
                        .addResultReader(InternalBinaryRange::new));
        registerAggregation(new AggregationSpec(HistogramAggregationBuilder::new, new HistogramParser(),
                HistogramAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalHistogram::new));
        registerAggregation(new AggregationSpec(DateHistogramAggregationBuilder::new, new DateHistogramParser(),
                DateHistogramAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalDateHistogram::new));
        registerAggregation(new AggregationSpec(GeoDistanceAggregationBuilder::new, new GeoDistanceParser(),
                GeoDistanceAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalGeoDistance::new));
        registerAggregation(new AggregationSpec(GeoGridAggregationBuilder::new, new GeoHashGridParser(),
                GeoGridAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalGeoHashGrid::new));
        registerAggregation(new AggregationSpec(NestedAggregationBuilder::new, NestedAggregationBuilder::parse,
                NestedAggregationBuilder.AGGREGATION_FIELD_NAME).addResultReader(InternalNested::new));
        registerAggregation(new AggregationSpec(ReverseNestedAggregationBuilder::new, ReverseNestedAggregationBuilder::parse,
                ReverseNestedAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalReverseNested::new));
        registerAggregation(new AggregationSpec(TopHitsAggregationBuilder::new, TopHitsAggregationBuilder::parse,
                TopHitsAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalTopHits::new));
        registerAggregation(new AggregationSpec(GeoBoundsAggregationBuilder::new, new GeoBoundsParser(),
                GeoBoundsAggregationBuilder.AGGREGATION_NAME_FIED).addResultReader(InternalGeoBounds::new));
        registerAggregation(new AggregationSpec(GeoCentroidAggregationBuilder::new, new GeoCentroidParser(),
                GeoCentroidAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalGeoCentroid::new));
        registerAggregation(new AggregationSpec(ScriptedMetricAggregationBuilder::new, ScriptedMetricAggregationBuilder::parse,
                ScriptedMetricAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalScriptedMetric::new));
        registerAggregation(new AggregationSpec(ChildrenAggregationBuilder::new, ChildrenAggregationBuilder::parse,
                ChildrenAggregationBuilder.AGGREGATION_NAME_FIELD).addResultReader(InternalChildren::new));

        registerPipelineAggregation(new PipelineAggregationSpec(
                DerivativePipelineAggregationBuilder::new,
                DerivativePipelineAggregator::new,
                DerivativePipelineAggregationBuilder::parse,
                DerivativePipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(InternalDerivative::new));
        registerPipelineAggregation(new PipelineAggregationSpec(
                MaxBucketPipelineAggregationBuilder::new,
                MaxBucketPipelineAggregator::new,
                MaxBucketPipelineAggregationBuilder.PARSER,
                MaxBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    // This bucket is used by many pipeline aggreations.
                    .addResultReader(InternalBucketMetricValue.NAME, InternalBucketMetricValue::new));
        registerPipelineAggregation(new PipelineAggregationSpec(
                MinBucketPipelineAggregationBuilder::new,
                MinBucketPipelineAggregator::new,
                MinBucketPipelineAggregationBuilder.PARSER,
                MinBucketPipelineAggregationBuilder.AGGREGATION_FIELD_NAME)
                    /* Uses InternalBucketMetricValue */);
        registerPipelineAggregation(new PipelineAggregationSpec(
                AvgBucketPipelineAggregationBuilder::new,
                AvgBucketPipelineAggregator::new,
                AvgBucketPipelineAggregationBuilder.PARSER,
                AvgBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    // This bucket is used by many pipeline aggreations.
                    .addResultReader(InternalSimpleValue.NAME, InternalSimpleValue::new));
        registerPipelineAggregation(new PipelineAggregationSpec(
                SumBucketPipelineAggregationBuilder::new,
                SumBucketPipelineAggregator::new,
                SumBucketPipelineAggregationBuilder.PARSER,
                SumBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    /* Uses InternalSimpleValue */);
        registerPipelineAggregation(new PipelineAggregationSpec(
                StatsBucketPipelineAggregationBuilder::new,
                StatsBucketPipelineAggregator::new,
                StatsBucketPipelineAggregationBuilder.PARSER,
                StatsBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(InternalStatsBucket::new));
        registerPipelineAggregation(new PipelineAggregationSpec(
                ExtendedStatsBucketPipelineAggregationBuilder::new,
                ExtendedStatsBucketPipelineAggregator::new,
                new ExtendedStatsBucketParser(),
                ExtendedStatsBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(InternalExtendedStatsBucket::new));
        registerPipelineAggregation(new PipelineAggregationSpec(
                PercentilesBucketPipelineAggregationBuilder::new,
                PercentilesBucketPipelineAggregator::new,
                PercentilesBucketPipelineAggregationBuilder.PARSER,
                PercentilesBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD)
                    .addResultReader(InternalPercentilesBucket::new));
        registerPipelineAggregation(new PipelineAggregationSpec(
                MovAvgPipelineAggregationBuilder::new,
                MovAvgPipelineAggregator::new,
                (n, c) -> MovAvgPipelineAggregationBuilder.parse(movingAverageModelParserRegistry, n, c),
                MovAvgPipelineAggregationBuilder.AGGREGATION_FIELD_NAME)
                    /* Uses InternalHistogram for buckets */);
        registerPipelineAggregation(new PipelineAggregationSpec(
                CumulativeSumPipelineAggregationBuilder::new,
                CumulativeSumPipelineAggregator::new,
                CumulativeSumPipelineAggregationBuilder::parse,
                CumulativeSumPipelineAggregationBuilder.AGGREGATION_NAME_FIELD));
        registerPipelineAggregation(new PipelineAggregationSpec(
                BucketScriptPipelineAggregationBuilder::new,
                BucketScriptPipelineAggregator::new,
                BucketScriptPipelineAggregationBuilder::parse,
                BucketScriptPipelineAggregationBuilder.AGGREGATION_NAME_FIELD));
        registerPipelineAggregation(new PipelineAggregationSpec(
                BucketSelectorPipelineAggregationBuilder::new,
                BucketSelectorPipelineAggregator::new,
                BucketSelectorPipelineAggregationBuilder::parse,
                BucketSelectorPipelineAggregationBuilder.AGGREGATION_NAME_FIELD));
        registerPipelineAggregation(new PipelineAggregationSpec(
                SerialDiffPipelineAggregationBuilder::new,
                SerialDiffPipelineAggregator::new,
                SerialDiffPipelineAggregationBuilder::parse,
                SerialDiffPipelineAggregationBuilder.AGGREGATION_NAME_FIELD));
    }

    protected void configureSearch() {
        // configure search private classes...
        bind(SearchPhaseController.class).asEagerSingleton();
        bind(FetchPhase.class).toInstance(new FetchPhase(fetchSubPhases));
        bind(SearchTransportService.class).asEagerSingleton();
        if (searchServiceImpl == SearchService.class) {
            bind(SearchService.class).asEagerSingleton();
        } else {
            bind(SearchService.class).to(searchServiceImpl).asEagerSingleton();
        }
    }

    private void registerShapes() {
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            ShapeBuilders.register(namedWriteables);
        }
    }

    private void registerRescorers() {
        namedWriteables.add(new Entry(RescoreBuilder.class, QueryRescorerBuilder.NAME, QueryRescorerBuilder::new));
    }

    private void registerSorts() {
        namedWriteables.add(new Entry(SortBuilder.class, GeoDistanceSortBuilder.NAME, GeoDistanceSortBuilder::new));
        namedWriteables.add(new Entry(SortBuilder.class, ScoreSortBuilder.NAME, ScoreSortBuilder::new));
        namedWriteables.add(new Entry(SortBuilder.class, ScriptSortBuilder.NAME, ScriptSortBuilder::new));
        namedWriteables.add(new Entry(SortBuilder.class, FieldSortBuilder.NAME, FieldSortBuilder::new));
    }

    private <T> void registerFromPlugin(List<SearchPlugin> plugins, Function<SearchPlugin, List<T>> producer, Consumer<T> consumer) {
        for (SearchPlugin plugin : plugins) {
            for (T t : producer.apply(plugin)) {
                consumer.accept(t);
            }
        }
    }

    public static void registerSmoothingModels(List<Entry> namedWriteables) {
        namedWriteables.add(new Entry(SmoothingModel.class, Laplace.NAME, Laplace::new));
        namedWriteables.add(new Entry(SmoothingModel.class, LinearInterpolation.NAME, LinearInterpolation::new));
        namedWriteables.add(new Entry(SmoothingModel.class, StupidBackoff.NAME, StupidBackoff::new));
    }

    private Map<String, Suggester<?>> setupSuggesters(List<SearchPlugin> plugins) {
        registerSmoothingModels(namedWriteables);

        // Suggester<?> is weird - it is both a Parser and a reader....
        NamedRegistry<Suggester<?>> suggesters = new NamedRegistry<Suggester<?>>("suggester") {
            @Override
            public void register(String name, Suggester<?> t) {
                super.register(name, t);
                namedWriteables.add(new Entry(SuggestionBuilder.class, name, t));
            }
        };
        suggesters.register("phrase", PhraseSuggester.INSTANCE);
        suggesters.register("term", TermSuggester.INSTANCE);
        suggesters.register("completion", CompletionSuggester.INSTANCE);

        suggesters.extractAndRegister(plugins, SearchPlugin::getSuggesters);
        return unmodifiableMap(suggesters.getRegistry());
    }

    private Map<String, Highlighter> setupHighlighters(Settings settings, List<SearchPlugin> plugins) {
        NamedRegistry<Highlighter> highlighters = new NamedRegistry<>("highlighter");
        highlighters.register("fvh",  new FastVectorHighlighter(settings));
        highlighters.register("plain", new PlainHighlighter());
        highlighters.register("postings", new PostingsHighlighter());

        highlighters.extractAndRegister(plugins, SearchPlugin::getHighlighters);

        return unmodifiableMap(highlighters.getRegistry());
    }

    private void registerScoreFunctions(List<SearchPlugin> plugins) {
        registerScoreFunction(new ScoreFunctionSpec<>(ScriptScoreFunctionBuilder.NAME, ScriptScoreFunctionBuilder::new,
                ScriptScoreFunctionBuilder::fromXContent));
        registerScoreFunction(
                new ScoreFunctionSpec<>(GaussDecayFunctionBuilder.NAME, GaussDecayFunctionBuilder::new, GaussDecayFunctionBuilder.PARSER));
        registerScoreFunction(new ScoreFunctionSpec<>(LinearDecayFunctionBuilder.NAME, LinearDecayFunctionBuilder::new,
                LinearDecayFunctionBuilder.PARSER));
        registerScoreFunction(new ScoreFunctionSpec<>(ExponentialDecayFunctionBuilder.NAME, ExponentialDecayFunctionBuilder::new,
                ExponentialDecayFunctionBuilder.PARSER));
        registerScoreFunction(new ScoreFunctionSpec<>(RandomScoreFunctionBuilder.NAME, RandomScoreFunctionBuilder::new,
                RandomScoreFunctionBuilder::fromXContent));
        registerScoreFunction(new ScoreFunctionSpec<>(FieldValueFactorFunctionBuilder.NAME, FieldValueFactorFunctionBuilder::new,
                FieldValueFactorFunctionBuilder::fromXContent));

        //weight doesn't have its own parser, so every function supports it out of the box.
        //Can be a single function too when not associated to any other function, which is why it needs to be registered manually here.
        namedWriteables.add(new Entry(ScoreFunctionBuilder.class, WeightBuilder.NAME, WeightBuilder::new));

        registerFromPlugin(plugins, SearchPlugin::getScoreFunctions, this::registerScoreFunction);
    }

    private void registerScoreFunction(ScoreFunctionSpec<?> scoreFunction) {
        scoreFunctionParserRegistry.register(scoreFunction.getParser(), scoreFunction.getName());
        namedWriteables.add(new Entry(ScoreFunctionBuilder.class, scoreFunction.getName().getPreferredName(), scoreFunction.getReader()));
    }

    private void registerValueFormats() {
        registerValueFormat(DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN);
        registerValueFormat(DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new);
        registerValueFormat(DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new);
        registerValueFormat(DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH);
        registerValueFormat(DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP);
        registerValueFormat(DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW);
    }

    /**
     * Register a new ValueFormat.
     */
    private void registerValueFormat(String name, Writeable.Reader<? extends DocValueFormat> reader) {
        namedWriteables.add(new Entry(DocValueFormat.class, name, reader));
    }

    private void registerSignificanceHeuristics(List<SearchPlugin> plugins) {
        registerSignificanceHeuristic(new SearchExtensionSpec<>(ChiSquare.NAME, ChiSquare::new, ChiSquare.PARSER));
        registerSignificanceHeuristic(new SearchExtensionSpec<>(GND.NAME, GND::new, GND.PARSER));
        registerSignificanceHeuristic(new SearchExtensionSpec<>(JLHScore.NAME, JLHScore::new, JLHScore::parse));
        registerSignificanceHeuristic(new SearchExtensionSpec<>(MutualInformation.NAME, MutualInformation::new, MutualInformation.PARSER));
        registerSignificanceHeuristic(new SearchExtensionSpec<>(PercentageScore.NAME, PercentageScore::new, PercentageScore::parse));
        registerSignificanceHeuristic(new SearchExtensionSpec<>(ScriptHeuristic.NAME, ScriptHeuristic::new, ScriptHeuristic::parse));

        registerFromPlugin(plugins, SearchPlugin::getSignificanceHeuristics, this::registerSignificanceHeuristic);
    }

    private void registerSignificanceHeuristic(SearchExtensionSpec<SignificanceHeuristic, SignificanceHeuristicParser> heuristic) {
        significanceHeuristicParserRegistry.register(heuristic.getParser(), heuristic.getName());
        namedWriteables.add(new Entry(SignificanceHeuristic.class, heuristic.getName().getPreferredName(), heuristic.getReader()));
    }

    private void registerMovingAverageModels(List<SearchPlugin> plugins) {
        registerMovingAverageModel(new SearchExtensionSpec<>(SimpleModel.NAME, SimpleModel::new, SimpleModel.PARSER));
        registerMovingAverageModel(new SearchExtensionSpec<>(LinearModel.NAME, LinearModel::new, LinearModel.PARSER));
        registerMovingAverageModel(new SearchExtensionSpec<>(EwmaModel.NAME, EwmaModel::new, EwmaModel.PARSER));
        registerMovingAverageModel(new SearchExtensionSpec<>(HoltLinearModel.NAME, HoltLinearModel::new, HoltLinearModel.PARSER));
        registerMovingAverageModel(new SearchExtensionSpec<>(HoltWintersModel.NAME, HoltWintersModel::new, HoltWintersModel.PARSER));

        registerFromPlugin(plugins, SearchPlugin::getMovingAverageModels, this::registerMovingAverageModel);
    }

    private void registerMovingAverageModel(SearchExtensionSpec<MovAvgModel, MovAvgModel.AbstractModelParser> movAvgModel) {
        movingAverageModelParserRegistry.register(movAvgModel.getParser(), movAvgModel.getName());
        namedWriteables.add(new Entry(MovAvgModel.class, movAvgModel.getName().getPreferredName(), movAvgModel.getReader()));
    }

    private void registerFetchSubPhases(List<SearchPlugin> plugins) {
        registerFetchSubPhase(new ExplainFetchSubPhase());
        registerFetchSubPhase(new DocValueFieldsFetchSubPhase());
        registerFetchSubPhase(new ScriptFieldsFetchSubPhase());
        registerFetchSubPhase(new FetchSourceSubPhase());
        registerFetchSubPhase(new VersionFetchSubPhase());
        registerFetchSubPhase(new MatchedQueriesFetchSubPhase());
        registerFetchSubPhase(new HighlightPhase(settings, highlighters));
        registerFetchSubPhase(new ParentFieldSubFetchPhase());

        FetchPhaseConstructionContext context = new FetchPhaseConstructionContext(highlighters);
        registerFromPlugin(plugins, p -> p.getFetchSubPhases(context), this::registerFetchSubPhase);
    }

    private void registerFetchSubPhase(FetchSubPhase subPhase) {
        Class<?> subPhaseClass = subPhase.getClass();
        if (fetchSubPhases.stream().anyMatch(p -> p.getClass().equals(subPhaseClass))) {
            throw new IllegalArgumentException("FetchSubPhase [" + subPhaseClass + "] already registered");
        }
        fetchSubPhases.add(requireNonNull(subPhase, "FetchSubPhase must not be null"));
    }

    private void registerQueryParsers(List<SearchPlugin> plugins) {
        registerQuery(new QuerySpec<>(MatchQueryBuilder.QUERY_NAME_FIELD, MatchQueryBuilder::new, MatchQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchPhraseQueryBuilder.NAME, MatchPhraseQueryBuilder::new, MatchPhraseQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchPhrasePrefixQueryBuilder.NAME, MatchPhrasePrefixQueryBuilder::new,
                MatchPhrasePrefixQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MultiMatchQueryBuilder.NAME, MultiMatchQueryBuilder::new, MultiMatchQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(NestedQueryBuilder.NAME, NestedQueryBuilder::new, NestedQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(HasChildQueryBuilder.NAME, HasChildQueryBuilder::new, HasChildQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(HasParentQueryBuilder.NAME, HasParentQueryBuilder::new, HasParentQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(DisMaxQueryBuilder.NAME, DisMaxQueryBuilder::new, DisMaxQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(IdsQueryBuilder.NAME, IdsQueryBuilder::new, IdsQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new, MatchAllQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(QueryStringQueryBuilder.NAME, QueryStringQueryBuilder::new, QueryStringQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(BoostingQueryBuilder.NAME, BoostingQueryBuilder::new, BoostingQueryBuilder::fromXContent));
        BooleanQuery.setMaxClauseCount(INDICES_MAX_CLAUSE_COUNT_SETTING.get(settings));
        registerQuery(new QuerySpec<>(BoolQueryBuilder.NAME, BoolQueryBuilder::new, BoolQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(TermQueryBuilder.NAME, TermQueryBuilder::new, TermQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(TermsQueryBuilder.QUERY_NAME_FIELD, TermsQueryBuilder::new, TermsQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(FuzzyQueryBuilder.NAME, FuzzyQueryBuilder::new, FuzzyQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(RegexpQueryBuilder.NAME, RegexpQueryBuilder::new, RegexpQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(RangeQueryBuilder.NAME, RangeQueryBuilder::new, RangeQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(PrefixQueryBuilder.NAME, PrefixQueryBuilder::new, PrefixQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(WildcardQueryBuilder.NAME, WildcardQueryBuilder::new, WildcardQueryBuilder::fromXContent));
        registerQuery(
                new QuerySpec<>(ConstantScoreQueryBuilder.NAME, ConstantScoreQueryBuilder::new, ConstantScoreQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanTermQueryBuilder.NAME, SpanTermQueryBuilder::new, SpanTermQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanNotQueryBuilder.NAME, SpanNotQueryBuilder::new, SpanNotQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanWithinQueryBuilder.NAME, SpanWithinQueryBuilder::new, SpanWithinQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanContainingQueryBuilder.NAME, SpanContainingQueryBuilder::new,
                SpanContainingQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(FieldMaskingSpanQueryBuilder.NAME, FieldMaskingSpanQueryBuilder::new,
                FieldMaskingSpanQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanFirstQueryBuilder.NAME, SpanFirstQueryBuilder::new, SpanFirstQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanNearQueryBuilder.NAME, SpanNearQueryBuilder::new, SpanNearQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanOrQueryBuilder.NAME, SpanOrQueryBuilder::new, SpanOrQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MoreLikeThisQueryBuilder.QUERY_NAME_FIELD, MoreLikeThisQueryBuilder::new,
                MoreLikeThisQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(WrapperQueryBuilder.NAME, WrapperQueryBuilder::new, WrapperQueryBuilder::fromXContent));
        // TODO Remove IndicesQuery in 6.0
        registerQuery(new QuerySpec<>(IndicesQueryBuilder.NAME, IndicesQueryBuilder::new, IndicesQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(CommonTermsQueryBuilder.NAME, CommonTermsQueryBuilder::new, CommonTermsQueryBuilder::fromXContent));
        registerQuery(
                new QuerySpec<>(SpanMultiTermQueryBuilder.NAME, SpanMultiTermQueryBuilder::new, SpanMultiTermQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(FunctionScoreQueryBuilder.NAME, FunctionScoreQueryBuilder::new,
                c -> FunctionScoreQueryBuilder.fromXContent(scoreFunctionParserRegistry, c)));
        registerQuery(
                new QuerySpec<>(SimpleQueryStringBuilder.NAME, SimpleQueryStringBuilder::new, SimpleQueryStringBuilder::fromXContent));
        registerQuery(new QuerySpec<>(TypeQueryBuilder.NAME, TypeQueryBuilder::new, TypeQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(ScriptQueryBuilder.NAME, ScriptQueryBuilder::new, ScriptQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(GeoDistanceQueryBuilder.NAME, GeoDistanceQueryBuilder::new, GeoDistanceQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(GeoDistanceRangeQueryBuilder.NAME, GeoDistanceRangeQueryBuilder::new,
                GeoDistanceRangeQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(GeoBoundingBoxQueryBuilder.QUERY_NAME_FIELD, GeoBoundingBoxQueryBuilder::new,
                GeoBoundingBoxQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(GeohashCellQuery.NAME, GeohashCellQuery.Builder::new, GeohashCellQuery.Builder::fromXContent));
        registerQuery(new QuerySpec<>(GeoPolygonQueryBuilder.NAME, GeoPolygonQueryBuilder::new, GeoPolygonQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(ExistsQueryBuilder.NAME, ExistsQueryBuilder::new, ExistsQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchNoneQueryBuilder.NAME, MatchNoneQueryBuilder::new, MatchNoneQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(ParentIdQueryBuilder.NAME, ParentIdQueryBuilder::new, ParentIdQueryBuilder::fromXContent));

        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            registerQuery(new QuerySpec<>(GeoShapeQueryBuilder.NAME, GeoShapeQueryBuilder::new, GeoShapeQueryBuilder::fromXContent));
        }

        registerFromPlugin(plugins, SearchPlugin::getQueries, this::registerQuery);
    }

    private void registerQuery(QuerySpec<?> spec) {
        queryParserRegistry.register(spec.getParser(), spec.getName());
        namedWriteables.add(new Entry(QueryBuilder.class, spec.getName().getPreferredName(), spec.getReader()));
    }
}
