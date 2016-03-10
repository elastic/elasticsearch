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
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.builders.CircleBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryParser;
import org.elasticsearch.index.query.BoostingQueryParser;
import org.elasticsearch.index.query.CommonTermsQueryParser;
import org.elasticsearch.index.query.ConstantScoreQueryParser;
import org.elasticsearch.index.query.DisMaxQueryParser;
import org.elasticsearch.index.query.EmptyQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryParser;
import org.elasticsearch.index.query.FieldMaskingSpanQueryParser;
import org.elasticsearch.index.query.FuzzyQueryParser;
import org.elasticsearch.index.query.GeoBoundingBoxQueryParser;
import org.elasticsearch.index.query.GeoDistanceQueryParser;
import org.elasticsearch.index.query.GeoDistanceRangeQueryParser;
import org.elasticsearch.index.query.GeoPolygonQueryParser;
import org.elasticsearch.index.query.GeoShapeQueryParser;
import org.elasticsearch.index.query.GeohashCellQuery;
import org.elasticsearch.index.query.HasChildQueryParser;
import org.elasticsearch.index.query.HasParentQueryParser;
import org.elasticsearch.index.query.IdsQueryParser;
import org.elasticsearch.index.query.IndicesQueryParser;
import org.elasticsearch.index.query.MatchAllQueryParser;
import org.elasticsearch.index.query.MatchNoneQueryParser;
import org.elasticsearch.index.query.MatchQueryParser;
import org.elasticsearch.index.query.MoreLikeThisQueryParser;
import org.elasticsearch.index.query.MultiMatchQueryParser;
import org.elasticsearch.index.query.NestedQueryParser;
import org.elasticsearch.index.query.ParentIdQueryParser;
import org.elasticsearch.index.query.PrefixQueryParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryStringQueryParser;
import org.elasticsearch.index.query.RangeQueryParser;
import org.elasticsearch.index.query.RegexpQueryParser;
import org.elasticsearch.index.query.ScriptQueryParser;
import org.elasticsearch.index.query.SimpleQueryStringParser;
import org.elasticsearch.index.query.SpanContainingQueryParser;
import org.elasticsearch.index.query.SpanFirstQueryParser;
import org.elasticsearch.index.query.SpanMultiTermQueryParser;
import org.elasticsearch.index.query.SpanNearQueryParser;
import org.elasticsearch.index.query.SpanNotQueryParser;
import org.elasticsearch.index.query.SpanOrQueryParser;
import org.elasticsearch.index.query.SpanTermQueryParser;
import org.elasticsearch.index.query.SpanWithinQueryParser;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.index.query.TermQueryParser;
import org.elasticsearch.index.query.TermsQueryParser;
import org.elasticsearch.index.query.TypeQueryParser;
import org.elasticsearch.index.query.WildcardQueryParser;
import org.elasticsearch.index.query.WrapperQueryParser;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParserMapper;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionParser;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionParser;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.weight.WeightBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.search.aggregations.AggregationBinaryParseElement;
import org.elasticsearch.search.aggregations.AggregationParseElement;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.bucket.children.ChildrenParser;
import org.elasticsearch.search.aggregations.bucket.children.InternalChildren;
import org.elasticsearch.search.aggregations.bucket.filter.FilterParser;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersParser;
import org.elasticsearch.search.aggregations.bucket.filters.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridParser;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalParser;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramParser;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingParser;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedParser;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedParser;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeParser;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeParser;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceParser;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.InternalGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.InternalIPv4Range;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.IpRangeParser;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedSamplerParser;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerParser;
import org.elasticsearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsParser;
import org.elasticsearch.search.aggregations.bucket.significant.UnmappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParserMapper;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsParser;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.metrics.avg.AvgParser;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityParser;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsParser;
import org.elasticsearch.search.aggregations.metrics.geobounds.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidParser;
import org.elasticsearch.search.aggregations.metrics.geocentroid.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxParser;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.min.MinParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesParser;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.InternalScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricParser;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsParser;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsParser;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumParser;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsParser;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountParser;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketParser;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptParser;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorParser;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumParser;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativeParser;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.InternalDerivative;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgParser;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelParserMapper;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffParser;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregator;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.parent.ParentFieldSubFetchPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.Highlighters;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.Suggesters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 *
 */
public class SearchModule extends AbstractModule {

    private final Set<Aggregator.Parser> aggParsers = new HashSet<>();
    private final Set<PipelineAggregator.Parser> pipelineAggParsers = new HashSet<>();
    private final Highlighters highlighters = new Highlighters();
    private final Suggesters suggesters = new Suggesters();
    /**
     * Function score parsers constructed on registration. This is ok because
     * they don't have any dependencies.
     */
    private final Map<String, ScoreFunctionParser<?>> functionScoreParsers = new HashMap<>();
    /**
     * Query parsers constructed at configure time. These have to be constructed
     * at configure time because they depend on things that are registered by
     * plugins (function score parsers).
     */
    private final List<Supplier<QueryParser<?>>> queryParsers = new ArrayList<>();
    private final Set<Class<? extends FetchSubPhase>> fetchSubPhases = new HashSet<>();
    private final Set<SignificanceHeuristicParser> heuristicParsers = new HashSet<>();
    private final Set<MovAvgModel.AbstractModelParser> modelParsers = new HashSet<>();

    private final Settings settings;
    private final NamedWriteableRegistry namedWriteableRegistry;

    // pkg private so tests can mock
    Class<? extends SearchService> searchServiceImpl = SearchService.class;

    public SearchModule(Settings settings, NamedWriteableRegistry namedWriteableRegistry) {
        this.settings = settings;
        this.namedWriteableRegistry = namedWriteableRegistry;

        registerBuiltinFunctionScoreParsers();
        registerBuiltinQueryParsers();
    }

    public void registerHighlighter(String key, Class<? extends Highlighter> clazz) {
        highlighters.registerExtension(key, clazz);
    }

    public void registerSuggester(String key, Class<? extends Suggester> suggester) {
        suggesters.registerExtension(key, suggester);
    }

    /**
     * Register a new ScoreFunctionParser.
     */
    public void registerFunctionScoreParser(ScoreFunctionParser<? extends ScoreFunctionBuilder> parser) {
        for (String name: parser.getNames()) {
            Object oldValue = functionScoreParsers.putIfAbsent(name, parser);
            if (oldValue != null) {
                throw new IllegalArgumentException("Function score parser [" + oldValue + "] already registered for name [" + name + "]");
            }
        }
        @SuppressWarnings("unchecked") NamedWriteable<? extends ScoreFunctionBuilder> sfb = parser.getBuilderPrototype();
        namedWriteableRegistry.registerPrototype(ScoreFunctionBuilder.class, sfb);
    }

    public void registerQueryParser(Supplier<QueryParser<?>> parser) {
        queryParsers.add(parser);
    }

    public void registerFetchSubPhase(Class<? extends FetchSubPhase> subPhase) {
        fetchSubPhases.add(subPhase);
    }

    public void registerHeuristicParser(SignificanceHeuristicParser parser) {
        heuristicParsers.add(parser);
    }

    public void registerModelParser(MovAvgModel.AbstractModelParser parser) {
        modelParsers.add(parser);
    }

    /**
     * Enabling extending the get module by adding a custom aggregation parser.
     *
     * @param parser The parser for the custom aggregator.
     */
    public void registerAggregatorParser(Aggregator.Parser parser) {
        aggParsers.add(parser);
    }

    public void registerPipelineParser(PipelineAggregator.Parser parser) {
        pipelineAggParsers.add(parser);
    }

    @Override
    protected void configure() {
        IndicesQueriesRegistry indicesQueriesRegistry = buildQueryParserRegistry();
        bind(IndicesQueriesRegistry.class).toInstance(indicesQueriesRegistry);
        configureSearch();
        configureAggs(indicesQueriesRegistry);
        configureHighlighters();
        configureSuggesters();
        configureFetchSubPhase();
        configureShapes();
        configureRescorers();
    }

    protected void configureFetchSubPhase() {
        Multibinder<FetchSubPhase> fetchSubPhaseMultibinder = Multibinder.newSetBinder(binder(), FetchSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(ExplainFetchSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(FieldDataFieldsFetchSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(ScriptFieldsFetchSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(FetchSourceSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(VersionFetchSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(MatchedQueriesFetchSubPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(HighlightPhase.class);
        fetchSubPhaseMultibinder.addBinding().to(ParentFieldSubFetchPhase.class);
        for (Class<? extends FetchSubPhase> clazz : fetchSubPhases) {
            fetchSubPhaseMultibinder.addBinding().to(clazz);
        }
        bind(InnerHitsFetchSubPhase.class).asEagerSingleton();
    }

    public IndicesQueriesRegistry buildQueryParserRegistry() {
        Map<String, QueryParser<?>> queryParsersMap = new HashMap<>();
        for (Supplier<QueryParser<?>> parserSupplier : queryParsers) {
            QueryParser<? extends QueryBuilder> parser = parserSupplier.get();
            for (String name: parser.names()) {
                Object oldValue = queryParsersMap.putIfAbsent(name, parser);
                if (oldValue != null) {
                    throw new IllegalArgumentException("Query parser [" + oldValue + "] already registered for name [" + name + "] while trying to register [" + parser + "]");
                }
            }
            @SuppressWarnings("unchecked") NamedWriteable<? extends QueryBuilder> qb = parser.getBuilderPrototype();
            namedWriteableRegistry.registerPrototype(QueryBuilder.class, qb);
        }
        return new IndicesQueriesRegistry(settings, queryParsersMap);
    }

    protected void configureSuggesters() {
        suggesters.bind(binder());
    }

    protected void configureHighlighters() {
       highlighters.bind(binder());
    }

    protected void configureAggs(IndicesQueriesRegistry indicesQueriesRegistry) {

        MovAvgModelParserMapper movAvgModelParserMapper = new MovAvgModelParserMapper(modelParsers);

        SignificanceHeuristicParserMapper significanceHeuristicParserMapper = new SignificanceHeuristicParserMapper(heuristicParsers);

        registerAggregatorParser(new AvgParser());
        registerAggregatorParser(new SumParser());
        registerAggregatorParser(new MinParser());
        registerAggregatorParser(new MaxParser());
        registerAggregatorParser(new StatsParser());
        registerAggregatorParser(new ExtendedStatsParser());
        registerAggregatorParser(new ValueCountParser());
        registerAggregatorParser(new PercentilesParser());
        registerAggregatorParser(new PercentileRanksParser());
        registerAggregatorParser(new CardinalityParser());
        registerAggregatorParser(new GlobalParser());
        registerAggregatorParser(new MissingParser());
        registerAggregatorParser(new FilterParser());
        registerAggregatorParser(new FiltersParser(indicesQueriesRegistry));
        registerAggregatorParser(new SamplerParser());
        registerAggregatorParser(new DiversifiedSamplerParser());
        registerAggregatorParser(new TermsParser());
        registerAggregatorParser(new SignificantTermsParser(significanceHeuristicParserMapper, indicesQueriesRegistry));
        registerAggregatorParser(new RangeParser());
        registerAggregatorParser(new DateRangeParser());
        registerAggregatorParser(new IpRangeParser());
        registerAggregatorParser(new HistogramParser());
        registerAggregatorParser(new DateHistogramParser());
        registerAggregatorParser(new GeoDistanceParser());
        registerAggregatorParser(new GeoHashGridParser());
        registerAggregatorParser(new NestedParser());
        registerAggregatorParser(new ReverseNestedParser());
        registerAggregatorParser(new TopHitsParser());
        registerAggregatorParser(new GeoBoundsParser());
        registerAggregatorParser(new GeoCentroidParser());
        registerAggregatorParser(new ScriptedMetricParser());
        registerAggregatorParser(new ChildrenParser());

        registerPipelineParser(new DerivativeParser());
        registerPipelineParser(new MaxBucketParser());
        registerPipelineParser(new MinBucketParser());
        registerPipelineParser(new AvgBucketParser());
        registerPipelineParser(new SumBucketParser());
        registerPipelineParser(new StatsBucketParser());
        registerPipelineParser(new ExtendedStatsBucketParser());
        registerPipelineParser(new PercentilesBucketParser());
        registerPipelineParser(new MovAvgParser(movAvgModelParserMapper));
        registerPipelineParser(new CumulativeSumParser());
        registerPipelineParser(new BucketScriptParser());
        registerPipelineParser(new BucketSelectorParser());
        registerPipelineParser(new SerialDiffParser());

        AggregatorParsers aggregatorParsers = new AggregatorParsers(aggParsers, pipelineAggParsers, namedWriteableRegistry);
        AggregationParseElement aggParseElement = new AggregationParseElement(aggregatorParsers, indicesQueriesRegistry);
        AggregationBinaryParseElement aggBinaryParseElement = new AggregationBinaryParseElement(aggregatorParsers, indicesQueriesRegistry);
        AggregationPhase aggPhase = new AggregationPhase(aggParseElement, aggBinaryParseElement);
        bind(AggregatorParsers.class).toInstance(aggregatorParsers);
        bind(AggregationParseElement.class).toInstance(aggParseElement);
        bind(AggregationPhase.class).toInstance(aggPhase);
    }

    protected void configureSearch() {
        // configure search private classes...
        bind(DfsPhase.class).asEagerSingleton();
        bind(QueryPhase.class).asEagerSingleton();
        bind(SearchPhaseController.class).asEagerSingleton();
        bind(FetchPhase.class).asEagerSingleton();
        bind(SearchTransportService.class).asEagerSingleton();
        if (searchServiceImpl == SearchService.class) {
            bind(SearchService.class).asEagerSingleton();
        } else {
            bind(SearchService.class).to(searchServiceImpl).asEagerSingleton();
        }
    }

    private void configureShapes() {
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, PointBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, CircleBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, EnvelopeBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, MultiPointBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, LineStringBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, MultiLineStringBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, PolygonBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, MultiPolygonBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, GeometryCollectionBuilder.PROTOTYPE);
        }
    }

    private void configureRescorers() {
        namedWriteableRegistry.registerPrototype(RescoreBuilder.class, QueryRescorerBuilder.PROTOTYPE);
    }

    private void registerBuiltinFunctionScoreParsers() {
        registerFunctionScoreParser(new ScriptScoreFunctionParser());
        registerFunctionScoreParser(new GaussDecayFunctionParser());
        registerFunctionScoreParser(new LinearDecayFunctionParser());
        registerFunctionScoreParser(new ExponentialDecayFunctionParser());
        registerFunctionScoreParser(new RandomScoreFunctionParser());
        registerFunctionScoreParser(new FieldValueFactorFunctionParser());
        //weight doesn't have its own parser, so every function supports it out of the box.
        //Can be a single function too when not associated to any other function, which is why it needs to be registered manually here.
        namedWriteableRegistry.registerPrototype(ScoreFunctionBuilder.class, new WeightBuilder());
    }

    private void registerBuiltinQueryParsers() {
        registerQueryParser(MatchQueryParser::new);
        registerQueryParser(MultiMatchQueryParser::new);
        registerQueryParser(NestedQueryParser::new);
        registerQueryParser(HasChildQueryParser::new);
        registerQueryParser(HasParentQueryParser::new);
        registerQueryParser(DisMaxQueryParser::new);
        registerQueryParser(IdsQueryParser::new);
        registerQueryParser(MatchAllQueryParser::new);
        registerQueryParser(QueryStringQueryParser::new);
        registerQueryParser(BoostingQueryParser::new);
        BooleanQuery.setMaxClauseCount(settings.getAsInt("index.query.bool.max_clause_count", settings.getAsInt("indices.query.bool.max_clause_count", BooleanQuery.getMaxClauseCount())));
        registerQueryParser(BoolQueryParser::new);
        registerQueryParser(TermQueryParser::new);
        registerQueryParser(TermsQueryParser::new);
        registerQueryParser(FuzzyQueryParser::new);
        registerQueryParser(RegexpQueryParser::new);
        registerQueryParser(RangeQueryParser::new);
        registerQueryParser(PrefixQueryParser::new);
        registerQueryParser(WildcardQueryParser::new);
        registerQueryParser(ConstantScoreQueryParser::new);
        registerQueryParser(SpanTermQueryParser::new);
        registerQueryParser(SpanNotQueryParser::new);
        registerQueryParser(SpanWithinQueryParser::new);
        registerQueryParser(SpanContainingQueryParser::new);
        registerQueryParser(FieldMaskingSpanQueryParser::new);
        registerQueryParser(SpanFirstQueryParser::new);
        registerQueryParser(SpanNearQueryParser::new);
        registerQueryParser(SpanOrQueryParser::new);
        registerQueryParser(MoreLikeThisQueryParser::new);
        registerQueryParser(WrapperQueryParser::new);
        registerQueryParser(IndicesQueryParser::new);
        registerQueryParser(CommonTermsQueryParser::new);
        registerQueryParser(SpanMultiTermQueryParser::new);
        // This is delayed until configure time to give plugins a chance to register parsers
        registerQueryParser(() -> new FunctionScoreQueryParser(new ScoreFunctionParserMapper(functionScoreParsers)));
        registerQueryParser(SimpleQueryStringParser::new);
        registerQueryParser(TemplateQueryParser::new);
        registerQueryParser(TypeQueryParser::new);
        registerQueryParser(ScriptQueryParser::new);
        registerQueryParser(GeoDistanceQueryParser::new);
        registerQueryParser(GeoDistanceRangeQueryParser::new);
        registerQueryParser(GeoBoundingBoxQueryParser::new);
        registerQueryParser(GeohashCellQuery.Parser::new);
        registerQueryParser(GeoPolygonQueryParser::new);
        registerQueryParser(ExistsQueryParser::new);
        registerQueryParser(MatchNoneQueryParser::new);
        registerQueryParser(ParentIdQueryParser::new);
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            registerQueryParser(GeoShapeQueryParser::new);
        }
        // EmptyQueryBuilder is not registered as query parser but used internally.
        // We need to register it with the NamedWriteableRegistry in order to serialize it
        namedWriteableRegistry.registerPrototype(QueryBuilder.class, EmptyQueryBuilder.PROTOTYPE);
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
        InternalIPv4Range.registerStream();
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
}
