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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParserMapper;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.search.action.SearchServiceTransportAction;
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
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerParser;
import org.elasticsearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsParser;
import org.elasticsearch.search.aggregations.bucket.significant.UnmappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParserMapper;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicStreams;
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
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumParser;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativeParser;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.derivative.InternalDerivative;
import org.elasticsearch.search.aggregations.pipeline.having.BucketSelectorParser;
import org.elasticsearch.search.aggregations.pipeline.having.BucketSelectorPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgParser;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelParserMapper;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelStreams;
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
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.Highlighters;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.Suggesters;

import java.util.*;

/**
 *
 */
public class SearchModule extends AbstractModule {

    private final Settings settings;
    private final Set<Class<? extends Aggregator.Parser>> aggParsers = new HashSet<>();
    private final Set<Class<? extends PipelineAggregator.Parser>> pipelineAggParsers = new HashSet<>();
    private final Highlighters highlighters = new Highlighters();
    private final Suggesters suggesters = new Suggesters();
    private final Set<Class<? extends ScoreFunctionParser>> functionScoreParsers = new HashSet<>();
    private final Set<Class<? extends FetchSubPhase>> fetchSubPhases = new HashSet<>();
    private final Set<Class<? extends SignificanceHeuristicParser>> heuristicParsers = new HashSet<>();
    private final Set<Class<? extends MovAvgModel.AbstractModelParser>> modelParsers = new HashSet<>();

    // pkg private so tests can mock
    Class<? extends SearchService> searchServiceImpl = SearchService.class;

    public SearchModule(Settings settings) {
        this.settings = settings;
    }

    // TODO document public API
    public void registerStream(SignificanceHeuristicStreams.Stream stream) {
        SignificanceHeuristicStreams.registerStream(stream);
    }

    public void registerStream(MovAvgModelStreams.Stream stream) {
        MovAvgModelStreams.registerStream(stream);
    }

    public void registerHighlighter(String key, Class<? extends Highlighter> clazz) {
        highlighters.registerExtension(key, clazz);
    }

    public void registerSuggester(String key, Class<? extends Suggester> suggester) {
        suggesters.registerExtension(key, suggester);
    }

    public void registerFunctionScoreParser(Class<? extends ScoreFunctionParser> parser) {
        functionScoreParsers.add(parser);
    }

    public void registerFetchSubPhase(Class<? extends FetchSubPhase> subPhase) {
        fetchSubPhases.add(subPhase);
    }

    public void registerHeuristicParser(Class<? extends SignificanceHeuristicParser> parser) {
        heuristicParsers.add(parser);
    }

    public void registerModelParser(Class<? extends MovAvgModel.AbstractModelParser> parser) {
        modelParsers.add(parser);
    }

    /**
     * Enabling extending the get module by adding a custom aggregation parser.
     *
     * @param parser The parser for the custom aggregator.
     */
    public void registerAggregatorParser(Class<? extends Aggregator.Parser> parser) {
        aggParsers.add(parser);
    }

    public void registerPipelineParser(Class<? extends PipelineAggregator.Parser> parser) {
        pipelineAggParsers.add(parser);
    }

    @Override
    protected void configure() {
        configureSearch();
        configureAggs();
        configureHighlighters();
        configureSuggesters();
        configureFunctionScore();
        configureFetchSubPhase();
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
        for (Class<? extends FetchSubPhase> clazz : fetchSubPhases) {
            fetchSubPhaseMultibinder.addBinding().to(clazz);
        }
        bind(InnerHitsFetchSubPhase.class).asEagerSingleton();
    }

    protected void configureSuggesters() {
        suggesters.bind(binder());
    }

    protected void configureFunctionScore() {
        Multibinder<ScoreFunctionParser> parserMapBinder = Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
        for (Class<? extends ScoreFunctionParser> clazz : functionScoreParsers) {
            parserMapBinder.addBinding().to(clazz);
        }
        bind(ScoreFunctionParserMapper.class);
    }

    protected void configureHighlighters() {
       highlighters.bind(binder());
    }

    protected void configureAggs() {
        Multibinder<Aggregator.Parser> multibinderAggParser = Multibinder.newSetBinder(binder(), Aggregator.Parser.class);
        multibinderAggParser.addBinding().to(AvgParser.class);
        multibinderAggParser.addBinding().to(SumParser.class);
        multibinderAggParser.addBinding().to(MinParser.class);
        multibinderAggParser.addBinding().to(MaxParser.class);
        multibinderAggParser.addBinding().to(StatsParser.class);
        multibinderAggParser.addBinding().to(ExtendedStatsParser.class);
        multibinderAggParser.addBinding().to(ValueCountParser.class);
        multibinderAggParser.addBinding().to(PercentilesParser.class);
        multibinderAggParser.addBinding().to(PercentileRanksParser.class);
        multibinderAggParser.addBinding().to(CardinalityParser.class);
        multibinderAggParser.addBinding().to(GlobalParser.class);
        multibinderAggParser.addBinding().to(MissingParser.class);
        multibinderAggParser.addBinding().to(FilterParser.class);
        multibinderAggParser.addBinding().to(FiltersParser.class);
        multibinderAggParser.addBinding().to(SamplerParser.class);
        multibinderAggParser.addBinding().to(TermsParser.class);
        multibinderAggParser.addBinding().to(SignificantTermsParser.class);
        multibinderAggParser.addBinding().to(RangeParser.class);
        multibinderAggParser.addBinding().to(DateRangeParser.class);
        multibinderAggParser.addBinding().to(IpRangeParser.class);
        multibinderAggParser.addBinding().to(HistogramParser.class);
        multibinderAggParser.addBinding().to(DateHistogramParser.class);
        multibinderAggParser.addBinding().to(GeoDistanceParser.class);
        multibinderAggParser.addBinding().to(GeoHashGridParser.class);
        multibinderAggParser.addBinding().to(NestedParser.class);
        multibinderAggParser.addBinding().to(ReverseNestedParser.class);
        multibinderAggParser.addBinding().to(TopHitsParser.class);
        multibinderAggParser.addBinding().to(GeoBoundsParser.class);
        multibinderAggParser.addBinding().to(ScriptedMetricParser.class);
        multibinderAggParser.addBinding().to(ChildrenParser.class);
        for (Class<? extends Aggregator.Parser> parser : aggParsers) {
            multibinderAggParser.addBinding().to(parser);
        }

        Multibinder<PipelineAggregator.Parser> multibinderPipelineAggParser = Multibinder.newSetBinder(binder(), PipelineAggregator.Parser.class);
        multibinderPipelineAggParser.addBinding().to(DerivativeParser.class);
        multibinderPipelineAggParser.addBinding().to(MaxBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(MinBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(AvgBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(SumBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(StatsBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(ExtendedStatsBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(PercentilesBucketParser.class);
        multibinderPipelineAggParser.addBinding().to(MovAvgParser.class);
        multibinderPipelineAggParser.addBinding().to(CumulativeSumParser.class);
        multibinderPipelineAggParser.addBinding().to(BucketScriptParser.class);
        multibinderPipelineAggParser.addBinding().to(BucketSelectorParser.class);
        multibinderPipelineAggParser.addBinding().to(SerialDiffParser.class);
        for (Class<? extends PipelineAggregator.Parser> parser : pipelineAggParsers) {
            multibinderPipelineAggParser.addBinding().to(parser);
        }
        bind(AggregatorParsers.class).asEagerSingleton();
        bind(AggregationParseElement.class).asEagerSingleton();
        bind(AggregationPhase.class).asEagerSingleton();

        Multibinder<SignificanceHeuristicParser> heuristicParserMultibinder = Multibinder.newSetBinder(binder(), SignificanceHeuristicParser.class);
        for (Class<? extends SignificanceHeuristicParser> clazz : heuristicParsers) {
            heuristicParserMultibinder.addBinding().to(clazz);
        }
        bind(SignificanceHeuristicParserMapper.class);

        Multibinder<MovAvgModel.AbstractModelParser> modelParserMultibinder = Multibinder.newSetBinder(binder(), MovAvgModel.AbstractModelParser.class);
        for (Class<? extends MovAvgModel.AbstractModelParser> clazz : modelParsers) {
            modelParserMultibinder.addBinding().to(clazz);
        }
        bind(MovAvgModelParserMapper.class);
    }

    protected void configureSearch() {
        // configure search private classes...
        bind(DfsPhase.class).asEagerSingleton();
        bind(QueryPhase.class).asEagerSingleton();
        bind(SearchPhaseController.class).asEagerSingleton();
        bind(FetchPhase.class).asEagerSingleton();
        bind(SearchServiceTransportAction.class).asEagerSingleton();
        bind(MoreLikeThisFetchService.class).asEagerSingleton();

        if (searchServiceImpl == SearchService.class) {
            bind(SearchService.class).asEagerSingleton();
        } else {
            bind(SearchService.class).to(searchServiceImpl).asEagerSingleton();
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
