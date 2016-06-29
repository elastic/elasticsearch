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

import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
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
import org.elasticsearch.index.query.QueryParser;
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
import org.elasticsearch.index.query.TemplateQueryBuilder;
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
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorParsers;
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
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingParser;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
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
import org.elasticsearch.search.aggregations.bucket.range.InternalBinaryRange;
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
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.InternalDerivative;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.EwmaModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltLinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.LinearModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregationBuilder;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.parent.ParentFieldSubFetchPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.Suggesters;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class SearchModule extends AbstractModule {

    private final Highlighters highlighters;
    private final Suggesters suggesters;
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

    private final Set<FetchSubPhase> fetchSubPhases = new HashSet<>();

    private final Settings settings;
    private final NamedWriteableRegistry namedWriteableRegistry;
    public static final Setting<Integer> INDICES_MAX_CLAUSE_COUNT_SETTING = Setting.intSetting("indices.query.bool.max_clause_count",
        1024, 1, Integer.MAX_VALUE, Setting.Property.NodeScope);

    // pkg private so tests can mock
    Class<? extends SearchService> searchServiceImpl = SearchService.class;

    public SearchModule(Settings settings, NamedWriteableRegistry namedWriteableRegistry) {
        this.settings = settings;
        this.namedWriteableRegistry = namedWriteableRegistry;
        suggesters = new Suggesters(namedWriteableRegistry);
        highlighters = new Highlighters(settings);
        registerBuiltinScoreFunctionParsers();
        registerBuiltinQueryParsers();
        registerBuiltinRescorers();
        registerBuiltinSorts();
        registerBuiltinValueFormats();
        registerBuiltinSignificanceHeuristics();
        registerBuiltinMovingAverageModels();
        registerBuiltinSubFetchPhases();
    }

    public void registerHighlighter(String key, Highlighter highligher) {
        highlighters.registerHighlighter(key, highligher);
    }

    public void registerSuggester(String key, Suggester<?> suggester) {
        suggesters.register(key, suggester);
    }

    /**
     * Register a new ScoreFunctionBuilder. Registration does two things:
     * <ul>
     * <li>Register the {@link ScoreFunctionParser} which parses XContent into a {@link ScoreFunctionBuilder} using its {@link ParseField}
     * </li>
     * <li>Register the {@link Writeable.Reader} which reads a stream representation of the builder under the
     * {@linkplain ParseField#getPreferredName()}.</li>
     * </ul>
     */
    public <T extends ScoreFunctionBuilder<T>> void registerScoreFunction(Writeable.Reader<T> reader, ScoreFunctionParser<T> parser,
            ParseField functionName) {
        scoreFunctionParserRegistry.register(parser, functionName);
        namedWriteableRegistry.register(ScoreFunctionBuilder.class, functionName.getPreferredName(), reader);
    }

    /**
     * Register a new ValueFormat.
     */
    // private for now, we can consider making it public if there are actual use cases for plugins
    // to register custom value formats
    private void registerValueFormat(String name, Writeable.Reader<? extends DocValueFormat> reader) {
        namedWriteableRegistry.register(DocValueFormat.class, name, reader);
    }

    /**
     * Register a query.
     *
     * @param reader the reader registered for this query's builder. Typically a reference to a constructor that takes a
     *        {@link org.elasticsearch.common.io.stream.StreamInput}
     * @param queryParser the parser the reads the query builder from xcontent
     * @param queryName holds the names by which this query might be parsed. The {@link ParseField#getPreferredName()} is special as it
     *        is the name by under which the reader is registered. So it is the name that the query should use as its
     *        {@link NamedWriteable#getWriteableName()} too.
     */
    public <QB extends QueryBuilder> void registerQuery(Writeable.Reader<QB> reader, QueryParser<QB> queryParser,
                                                                         ParseField queryName) {
        queryParserRegistry.register(queryParser, queryName);
        namedWriteableRegistry.register(QueryBuilder.class, queryName.getPreferredName(), reader);
    }

    public IndicesQueriesRegistry getQueryParserRegistry() {
        return queryParserRegistry;
    }

    /**
     * Registers a {@link FetchSubPhase} instance. This sub phase is executed when docuemnts are fetched for instanced to highlight
     * documents.
     */
    public void registerFetchSubPhase(FetchSubPhase subPhase) {
        fetchSubPhases.add(Objects.requireNonNull(subPhase, "FetchSubPhase must not be null"));
    }

    /**
     * Returns the {@link Highlighter} registry
     */
    public Highlighters getHighlighters() {
        return highlighters;
    }

    /**
     * Register a {@link SignificanceHeuristic}.
     *
     * @param heuristicName the name(s) at which the heuristic is parsed and streamed. The {@link ParseField#getPreferredName()} is the name
     *        under which it is streamed. All names work for the parser.
     * @param reader reads the heuristic from a stream
     * @param parser reads the heuristic from an XContentParser
     */
    public void registerSignificanceHeuristic(ParseField heuristicName, Writeable.Reader<SignificanceHeuristic> reader,
            SignificanceHeuristicParser parser) {
        significanceHeuristicParserRegistry.register(parser, heuristicName);
        namedWriteableRegistry.register(SignificanceHeuristic.class, heuristicName.getPreferredName(), reader);
    }

    /**
     * The registry of {@link SignificanceHeuristic}s.
     */
    public ParseFieldRegistry<SignificanceHeuristicParser> getSignificanceHeuristicParserRegistry() {
        return significanceHeuristicParserRegistry;
    }

    /**
     * Register a {@link MovAvgModel}.
     *
     * @param modelName the name(s) at which the model is parsed and streamed. The {@link ParseField#getPreferredName()} is the name under
     *        which it is streamed. All named work for the parser.
     * @param reader reads the model from a stream
     * @param parser reads the model from an XContentParser
     */
    public void registerMovingAverageModel(ParseField modelName, Writeable.Reader<MovAvgModel> reader,
            MovAvgModel.AbstractModelParser parser) {
        movingAverageModelParserRegistry.register(parser, modelName);
        namedWriteableRegistry.register(MovAvgModel.class, modelName.getPreferredName(), reader);
    }

    /**
     * The registry of {@link MovAvgModel}s.
     */
    public ParseFieldRegistry<MovAvgModel.AbstractModelParser> getMovingAverageMdelParserRegistry() {
        return movingAverageModelParserRegistry;
    }

    /**
     * Register an aggregation.
     *
     * @param reader reads the aggregation builder from a stream
     * @param aggregationParser reads the aggregation builder from XContent
     * @param aggregationName names by which the aggregation may be parsed. The first name is special because it is the name that the reader
     *        is registered under.
     */
    public void registerAggregation(Writeable.Reader<? extends AggregationBuilder> reader, Aggregator.Parser aggregationParser,
                                                                        ParseField aggregationName) {
        aggregationParserRegistry.register(aggregationParser, aggregationName);
        namedWriteableRegistry.register(AggregationBuilder.class, aggregationName.getPreferredName(), reader);
    }

    /**
     * Register a pipeline aggregation.
     *
     * @param reader reads the aggregation builder from a stream
     * @param aggregationParser reads the aggregation builder from XContent
     * @param aggregationName names by which the aggregation may be parsed. The first name is special because it is the name that the reader
     *        is registered under.
     */
    public void registerPipelineAggregation(Writeable.Reader<? extends PipelineAggregationBuilder> reader,
            PipelineAggregator.Parser aggregationParser, ParseField aggregationName) {
        pipelineAggregationParserRegistry.register(aggregationParser, aggregationName);
        namedWriteableRegistry.register(PipelineAggregationBuilder.class, aggregationName.getPreferredName(), reader);
    }

    @Override
    protected void configure() {
        bind(IndicesQueriesRegistry.class).toInstance(queryParserRegistry);
        bind(Suggesters.class).toInstance(suggesters);
        configureSearch();
        configureAggs();
        configureShapes();
    }

    protected void configureAggs() {
        registerAggregation(AvgAggregationBuilder::new, new AvgParser(), AvgAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(SumAggregationBuilder::new, new SumParser(), SumAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(MinAggregationBuilder::new, new MinParser(), MinAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(MaxAggregationBuilder::new, new MaxParser(), MaxAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(StatsAggregationBuilder::new, new StatsParser(), StatsAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(ExtendedStatsAggregationBuilder::new, new ExtendedStatsParser(),
        ExtendedStatsAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(ValueCountAggregationBuilder::new, new ValueCountParser(), ValueCountAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(PercentilesAggregationBuilder::new, new PercentilesParser(),
                PercentilesAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(PercentileRanksAggregationBuilder::new, new PercentileRanksParser(),
                PercentileRanksAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(CardinalityAggregationBuilder::new, new CardinalityParser(),
                CardinalityAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(GlobalAggregationBuilder::new, GlobalAggregationBuilder::parse,
                GlobalAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(MissingAggregationBuilder::new, new MissingParser(), MissingAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(FilterAggregationBuilder::new, FilterAggregationBuilder::parse,
                FilterAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(FiltersAggregationBuilder::new, FiltersAggregationBuilder::parse,
                FiltersAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(SamplerAggregationBuilder::new, SamplerAggregationBuilder::parse,
                SamplerAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(DiversifiedAggregationBuilder::new, new DiversifiedSamplerParser(),
                DiversifiedAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(TermsAggregationBuilder::new, new TermsParser(), TermsAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(SignificantTermsAggregationBuilder::new,
                new SignificantTermsParser(significanceHeuristicParserRegistry, queryParserRegistry),
                SignificantTermsAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(RangeAggregationBuilder::new, new RangeParser(), RangeAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(DateRangeAggregationBuilder::new, new DateRangeParser(), DateRangeAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(IpRangeAggregationBuilder::new, new IpRangeParser(), IpRangeAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(HistogramAggregationBuilder::new, new HistogramParser(), HistogramAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(DateHistogramAggregationBuilder::new, new DateHistogramParser(),
                DateHistogramAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(GeoDistanceAggregationBuilder::new, new GeoDistanceParser(),
                GeoDistanceAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(GeoGridAggregationBuilder::new, new GeoHashGridParser(), GeoGridAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(NestedAggregationBuilder::new, NestedAggregationBuilder::parse,
                NestedAggregationBuilder.AGGREGATION_FIELD_NAME);
        registerAggregation(ReverseNestedAggregationBuilder::new, ReverseNestedAggregationBuilder::parse,
                ReverseNestedAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(TopHitsAggregationBuilder::new, TopHitsAggregationBuilder::parse,
                TopHitsAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(GeoBoundsAggregationBuilder::new, new GeoBoundsParser(), GeoBoundsAggregationBuilder.AGGREGATION_NAME_FIED);
        registerAggregation(GeoCentroidAggregationBuilder::new, new GeoCentroidParser(),
                GeoCentroidAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(ScriptedMetricAggregationBuilder::new, ScriptedMetricAggregationBuilder::parse,
                ScriptedMetricAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerAggregation(ChildrenAggregationBuilder::new, ChildrenAggregationBuilder::parse,
                ChildrenAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(DerivativePipelineAggregationBuilder::new, DerivativePipelineAggregationBuilder::parse,
                DerivativePipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(MaxBucketPipelineAggregationBuilder::new, MaxBucketPipelineAggregationBuilder.PARSER,
                MaxBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(MinBucketPipelineAggregationBuilder::new, MinBucketPipelineAggregationBuilder.PARSER,
                MinBucketPipelineAggregationBuilder.AGGREGATION_FIELD_NAME);
        registerPipelineAggregation(AvgBucketPipelineAggregationBuilder::new, AvgBucketPipelineAggregationBuilder.PARSER,
                AvgBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(SumBucketPipelineAggregationBuilder::new, SumBucketPipelineAggregationBuilder.PARSER,
                SumBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(StatsBucketPipelineAggregationBuilder::new, StatsBucketPipelineAggregationBuilder.PARSER,
                StatsBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(ExtendedStatsBucketPipelineAggregationBuilder::new, new ExtendedStatsBucketParser(),
                ExtendedStatsBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(PercentilesBucketPipelineAggregationBuilder::new, PercentilesBucketPipelineAggregationBuilder.PARSER,
                PercentilesBucketPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(MovAvgPipelineAggregationBuilder::new,
                (n, c) -> MovAvgPipelineAggregationBuilder.parse(movingAverageModelParserRegistry, n, c),
                MovAvgPipelineAggregationBuilder.AGGREGATION_FIELD_NAME);
        registerPipelineAggregation(CumulativeSumPipelineAggregationBuilder::new, CumulativeSumPipelineAggregationBuilder::parse,
                CumulativeSumPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(BucketScriptPipelineAggregationBuilder::new, BucketScriptPipelineAggregationBuilder::parse,
                BucketScriptPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(BucketSelectorPipelineAggregationBuilder::new, BucketSelectorPipelineAggregationBuilder::parse,
                BucketSelectorPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        registerPipelineAggregation(SerialDiffPipelineAggregationBuilder::new, SerialDiffPipelineAggregationBuilder::parse,
                SerialDiffPipelineAggregationBuilder.AGGREGATION_NAME_FIELD);
        bind(AggregatorParsers.class).toInstance(aggregatorParsers);
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

    private void configureShapes() {
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            ShapeBuilders.register(namedWriteableRegistry);
        }
    }

    private void registerBuiltinRescorers() {
        namedWriteableRegistry.register(RescoreBuilder.class, QueryRescorerBuilder.NAME, QueryRescorerBuilder::new);
    }

    private void registerBuiltinSorts() {
        namedWriteableRegistry.register(SortBuilder.class, GeoDistanceSortBuilder.NAME, GeoDistanceSortBuilder::new);
        namedWriteableRegistry.register(SortBuilder.class, ScoreSortBuilder.NAME, ScoreSortBuilder::new);
        namedWriteableRegistry.register(SortBuilder.class, ScriptSortBuilder.NAME, ScriptSortBuilder::new);
        namedWriteableRegistry.register(SortBuilder.class, FieldSortBuilder.NAME, FieldSortBuilder::new);
    }

    private void registerBuiltinScoreFunctionParsers() {
        registerScoreFunction(ScriptScoreFunctionBuilder::new, ScriptScoreFunctionBuilder::fromXContent,
                ScriptScoreFunctionBuilder.FUNCTION_NAME_FIELD);
        registerScoreFunction(GaussDecayFunctionBuilder::new, GaussDecayFunctionBuilder.PARSER,
                GaussDecayFunctionBuilder.FUNCTION_NAME_FIELD);
        registerScoreFunction(LinearDecayFunctionBuilder::new, LinearDecayFunctionBuilder.PARSER,
                LinearDecayFunctionBuilder.FUNCTION_NAME_FIELD);
        registerScoreFunction(ExponentialDecayFunctionBuilder::new, ExponentialDecayFunctionBuilder.PARSER,
                ExponentialDecayFunctionBuilder.FUNCTION_NAME_FIELD);
        registerScoreFunction(RandomScoreFunctionBuilder::new, RandomScoreFunctionBuilder::fromXContent,
                RandomScoreFunctionBuilder.FUNCTION_NAME_FIELD);
        registerScoreFunction(FieldValueFactorFunctionBuilder::new, FieldValueFactorFunctionBuilder::fromXContent,
                FieldValueFactorFunctionBuilder.FUNCTION_NAME_FIELD);

        //weight doesn't have its own parser, so every function supports it out of the box.
        //Can be a single function too when not associated to any other function, which is why it needs to be registered manually here.
        namedWriteableRegistry.register(ScoreFunctionBuilder.class, WeightBuilder.NAME, WeightBuilder::new);
    }

    private void registerBuiltinValueFormats() {
        registerValueFormat(DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN);
        registerValueFormat(DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new);
        registerValueFormat(DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new);
        registerValueFormat(DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH);
        registerValueFormat(DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP);
        registerValueFormat(DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW);
    }

    private void registerBuiltinSignificanceHeuristics() {
        registerSignificanceHeuristic(ChiSquare.NAMES_FIELD, ChiSquare::new, ChiSquare.PARSER);
        registerSignificanceHeuristic(GND.NAMES_FIELD, GND::new, GND.PARSER);
        registerSignificanceHeuristic(JLHScore.NAMES_FIELD, JLHScore::new, JLHScore::parse);
        registerSignificanceHeuristic(MutualInformation.NAMES_FIELD, MutualInformation::new, MutualInformation.PARSER);
        registerSignificanceHeuristic(PercentageScore.NAMES_FIELD, PercentageScore::new, PercentageScore::parse);
        registerSignificanceHeuristic(ScriptHeuristic.NAMES_FIELD, ScriptHeuristic::new, ScriptHeuristic::parse);
    }

    private void registerBuiltinMovingAverageModels() {
        registerMovingAverageModel(SimpleModel.NAME_FIELD, SimpleModel::new, SimpleModel.PARSER);
        registerMovingAverageModel(LinearModel.NAME_FIELD, LinearModel::new, LinearModel.PARSER);
        registerMovingAverageModel(EwmaModel.NAME_FIELD, EwmaModel::new, EwmaModel.PARSER);
        registerMovingAverageModel(HoltLinearModel.NAME_FIELD, HoltLinearModel::new, HoltLinearModel.PARSER);
        registerMovingAverageModel(HoltWintersModel.NAME_FIELD, HoltWintersModel::new, HoltWintersModel.PARSER);
    }

    private void registerBuiltinSubFetchPhases() {
        registerFetchSubPhase(new ExplainFetchSubPhase());
        registerFetchSubPhase(new FieldDataFieldsFetchSubPhase());
        registerFetchSubPhase(new ScriptFieldsFetchSubPhase());
        registerFetchSubPhase(new FetchSourceSubPhase());
        registerFetchSubPhase(new VersionFetchSubPhase());
        registerFetchSubPhase(new MatchedQueriesFetchSubPhase());
        registerFetchSubPhase(new HighlightPhase(settings, highlighters));
        registerFetchSubPhase(new ParentFieldSubFetchPhase());
    }

    private void registerBuiltinQueryParsers() {
        registerQuery(MatchQueryBuilder::new, MatchQueryBuilder::fromXContent, MatchQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(MatchPhraseQueryBuilder::new, MatchPhraseQueryBuilder::fromXContent, MatchPhraseQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(MatchPhrasePrefixQueryBuilder::new, MatchPhrasePrefixQueryBuilder::fromXContent,
                MatchPhrasePrefixQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(MultiMatchQueryBuilder::new, MultiMatchQueryBuilder::fromXContent, MultiMatchQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(NestedQueryBuilder::new, NestedQueryBuilder::fromXContent, NestedQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(HasChildQueryBuilder::new, HasChildQueryBuilder::fromXContent, HasChildQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(HasParentQueryBuilder::new, HasParentQueryBuilder::fromXContent, HasParentQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(DisMaxQueryBuilder::new, DisMaxQueryBuilder::fromXContent, DisMaxQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(IdsQueryBuilder::new, IdsQueryBuilder::fromXContent, IdsQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(MatchAllQueryBuilder::new, MatchAllQueryBuilder::fromXContent, MatchAllQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(QueryStringQueryBuilder::new, QueryStringQueryBuilder::fromXContent, QueryStringQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(BoostingQueryBuilder::new, BoostingQueryBuilder::fromXContent, BoostingQueryBuilder.QUERY_NAME_FIELD);
        BooleanQuery.setMaxClauseCount(INDICES_MAX_CLAUSE_COUNT_SETTING.get(settings));
        registerQuery(BoolQueryBuilder::new, BoolQueryBuilder::fromXContent, BoolQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(TermQueryBuilder::new, TermQueryBuilder::fromXContent, TermQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(TermsQueryBuilder::new, TermsQueryBuilder::fromXContent, TermsQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(FuzzyQueryBuilder::new, FuzzyQueryBuilder::fromXContent, FuzzyQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(RegexpQueryBuilder::new, RegexpQueryBuilder::fromXContent, RegexpQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(RangeQueryBuilder::new, RangeQueryBuilder::fromXContent, RangeQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(PrefixQueryBuilder::new, PrefixQueryBuilder::fromXContent, PrefixQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(WildcardQueryBuilder::new, WildcardQueryBuilder::fromXContent, WildcardQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(ConstantScoreQueryBuilder::new, ConstantScoreQueryBuilder::fromXContent, ConstantScoreQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanTermQueryBuilder::new, SpanTermQueryBuilder::fromXContent, SpanTermQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanNotQueryBuilder::new, SpanNotQueryBuilder::fromXContent, SpanNotQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanWithinQueryBuilder::new, SpanWithinQueryBuilder::fromXContent, SpanWithinQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanContainingQueryBuilder::new, SpanContainingQueryBuilder::fromXContent,
                SpanContainingQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(FieldMaskingSpanQueryBuilder::new, FieldMaskingSpanQueryBuilder::fromXContent,
                FieldMaskingSpanQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanFirstQueryBuilder::new, SpanFirstQueryBuilder::fromXContent, SpanFirstQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanNearQueryBuilder::new, SpanNearQueryBuilder::fromXContent, SpanNearQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanOrQueryBuilder::new, SpanOrQueryBuilder::fromXContent, SpanOrQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(MoreLikeThisQueryBuilder::new, MoreLikeThisQueryBuilder::fromXContent, MoreLikeThisQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(WrapperQueryBuilder::new, WrapperQueryBuilder::fromXContent, WrapperQueryBuilder.QUERY_NAME_FIELD);
        // TODO Remove IndicesQuery in 6.0
        registerQuery(IndicesQueryBuilder::new, IndicesQueryBuilder::fromXContent, IndicesQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(CommonTermsQueryBuilder::new, CommonTermsQueryBuilder::fromXContent, CommonTermsQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SpanMultiTermQueryBuilder::new, SpanMultiTermQueryBuilder::fromXContent, SpanMultiTermQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(FunctionScoreQueryBuilder::new, c -> FunctionScoreQueryBuilder.fromXContent(scoreFunctionParserRegistry, c),
                FunctionScoreQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(SimpleQueryStringBuilder::new, SimpleQueryStringBuilder::fromXContent, SimpleQueryStringBuilder.QUERY_NAME_FIELD);
        registerQuery(TemplateQueryBuilder::new, TemplateQueryBuilder::fromXContent, TemplateQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(TypeQueryBuilder::new, TypeQueryBuilder::fromXContent, TypeQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(ScriptQueryBuilder::new, ScriptQueryBuilder::fromXContent, ScriptQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(GeoDistanceQueryBuilder::new, GeoDistanceQueryBuilder::fromXContent, GeoDistanceQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(GeoDistanceRangeQueryBuilder::new, GeoDistanceRangeQueryBuilder::fromXContent,
                GeoDistanceRangeQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(GeoBoundingBoxQueryBuilder::new, GeoBoundingBoxQueryBuilder::fromXContent,
                GeoBoundingBoxQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(GeohashCellQuery.Builder::new, GeohashCellQuery.Builder::fromXContent, GeohashCellQuery.QUERY_NAME_FIELD);
        registerQuery(GeoPolygonQueryBuilder::new, GeoPolygonQueryBuilder::fromXContent, GeoPolygonQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(ExistsQueryBuilder::new, ExistsQueryBuilder::fromXContent, ExistsQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(MatchNoneQueryBuilder::new, MatchNoneQueryBuilder::fromXContent, MatchNoneQueryBuilder.QUERY_NAME_FIELD);
        registerQuery(ParentIdQueryBuilder::new, ParentIdQueryBuilder::fromXContent, ParentIdQueryBuilder.QUERY_NAME_FIELD);

        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            registerQuery(GeoShapeQueryBuilder::new, GeoShapeQueryBuilder::fromXContent, GeoShapeQueryBuilder.QUERY_NAME_FIELD);
        }
    }

    static {
        // calcs
        InternalAvg.registerStreams();
        InternalSum.registerStreams();
        InternalMin.registerStreams();
        InternalMax.registerStreams();
        InternalStats.registerStreams();
        InternalExtendedStats.registerStreams();
        InternalValueCount.registerStreams();
        InternalTDigestPercentiles.registerStreams();
        InternalTDigestPercentileRanks.registerStreams();
        InternalHDRPercentiles.registerStreams();
        InternalHDRPercentileRanks.registerStreams();
        InternalCardinality.registerStreams();
        InternalScriptedMetric.registerStreams();
        InternalGeoCentroid.registerStreams();

        // buckets
        InternalGlobal.registerStreams();
        InternalFilter.registerStreams();
        InternalFilters.registerStream();
        InternalSampler.registerStreams();
        UnmappedSampler.registerStreams();
        InternalMissing.registerStreams();
        StringTerms.registerStreams();
        LongTerms.registerStreams();
        SignificantStringTerms.registerStreams();
        SignificantLongTerms.registerStreams();
        UnmappedSignificantTerms.registerStreams();
        InternalGeoHashGrid.registerStreams();
        DoubleTerms.registerStreams();
        UnmappedTerms.registerStreams();
        InternalRange.registerStream();
        InternalDateRange.registerStream();
        InternalBinaryRange.registerStream();
        InternalHistogram.registerStream();
        InternalGeoDistance.registerStream();
        InternalNested.registerStream();
        InternalReverseNested.registerStream();
        InternalTopHits.registerStreams();
        InternalGeoBounds.registerStream();
        InternalChildren.registerStream();

        // Pipeline Aggregations
        DerivativePipelineAggregator.registerStreams();
        InternalDerivative.registerStreams();
        InternalSimpleValue.registerStreams();
        InternalBucketMetricValue.registerStreams();
        MaxBucketPipelineAggregator.registerStreams();
        MinBucketPipelineAggregator.registerStreams();
        AvgBucketPipelineAggregator.registerStreams();
        SumBucketPipelineAggregator.registerStreams();
        StatsBucketPipelineAggregator.registerStreams();
        ExtendedStatsBucketPipelineAggregator.registerStreams();
        PercentilesBucketPipelineAggregator.registerStreams();
        MovAvgPipelineAggregator.registerStreams();
        CumulativeSumPipelineAggregator.registerStreams();
        BucketScriptPipelineAggregator.registerStreams();
        BucketSelectorPipelineAggregator.registerStreams();
        SerialDiffPipelineAggregator.registerStreams();
    }

    public Suggesters getSuggesters() {
        return suggesters;
    }
}
