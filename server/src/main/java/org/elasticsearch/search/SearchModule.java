/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.NamedRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.CommonTermsQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.DistanceFeatureQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.FieldMaskingSpanQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchBoolPrefixQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
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
import org.elasticsearch.index.query.SpanNearQueryBuilder.SpanGapQueryBuilder;
import org.elasticsearch.index.query.SpanNotQueryBuilder;
import org.elasticsearch.index.query.SpanOrQueryBuilder;
import org.elasticsearch.index.query.SpanTermQueryBuilder;
import org.elasticsearch.index.query.SpanWithinQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.TermsSetQueryBuilder;
import org.elasticsearch.index.query.TypeQueryV7Builder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.index.query.functionscore.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.WeightBuilder;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.plugins.SearchPlugin.AggregationSpec;
import org.elasticsearch.plugins.SearchPlugin.FetchPhaseConstructionContext;
import org.elasticsearch.plugins.SearchPlugin.PipelineAggregationSpec;
import org.elasticsearch.plugins.SearchPlugin.QuerySpec;
import org.elasticsearch.plugins.SearchPlugin.QueryVectorBuilderSpec;
import org.elasticsearch.plugins.SearchPlugin.RescorerSpec;
import org.elasticsearch.plugins.SearchPlugin.ScoreFunctionSpec;
import org.elasticsearch.plugins.SearchPlugin.SearchExtSpec;
import org.elasticsearch.plugins.SearchPlugin.SignificanceHeuristicSpec;
import org.elasticsearch.plugins.SearchPlugin.SuggesterSpec;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalVariableWidthHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.prefix.InternalIpPrefix;
import org.elasticsearch.search.aggregations.bucket.prefix.IpPrefixAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.InternalBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.InternalGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.random.InternalRandomSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.RareTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedSignificantTerms;
import org.elasticsearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.GND;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.PercentageScore;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.ScriptHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.InternalGeoBounds;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.InternalScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.InternalWeightedAvg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketParser;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.InternalExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.InternalPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.InternalStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MinBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MovAvgPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SerialDiffPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.ExplainPhase;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesPhase;
import org.elasticsearch.search.fetch.subphase.FetchFieldsPhase;
import org.elasticsearch.search.fetch.subphase.FetchScorePhase;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.fetch.subphase.FetchVersionPhase;
import org.elasticsearch.search.fetch.subphase.MatchedQueriesPhase;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsPhase;
import org.elasticsearch.search.fetch.subphase.SeqNoPrimaryTermPhase;
import org.elasticsearch.search.fetch.subphase.StoredFieldsPhase;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightPhase;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.PlainHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.UnifiedHighlighter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.Laplace;
import org.elasticsearch.search.suggest.phrase.LinearInterpolation;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.SmoothingModel;
import org.elasticsearch.search.suggest.phrase.StupidBackoff;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Sets up things that can be done at search time like queries, aggregations, and suggesters.
 */
public class SearchModule {
    public static final Setting<Integer> INDICES_MAX_CLAUSE_COUNT_SETTING = Setting.intSetting(
        "indices.query.bool.max_clause_count",
        4096,
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning
    );

    public static final Setting<Integer> INDICES_MAX_NESTED_DEPTH_SETTING = Setting.intSetting(
        "indices.query.bool.max_nested_depth",
        30,
        1,
        Integer.MAX_VALUE,
        Setting.Property.NodeScope
    );

    private final Map<String, Highlighter> highlighters;

    private final List<FetchSubPhase> fetchSubPhases = new ArrayList<>();

    private final Settings settings;
    private final List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
    private final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
    private final ValuesSourceRegistry valuesSourceRegistry;
    private final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> requestCacheKeyDifferentiator;

    /**
     * Constructs a new SearchModule object
     *
     * @param settings Current settings
     * @param plugins List of included {@link SearchPlugin} objects.
     */
    public SearchModule(Settings settings, List<SearchPlugin> plugins) {
        this.settings = settings;
        registerSuggesters(plugins);
        highlighters = setupHighlighters(settings, plugins);
        registerScoreFunctions(plugins);
        registerQueryParsers(plugins);
        registerRescorers(plugins);
        registerSorts();
        registerValueFormats();
        registerSignificanceHeuristics(plugins);
        registerQueryVectorBuilders(plugins);
        this.valuesSourceRegistry = registerAggregations(plugins);
        registerPipelineAggregations(plugins);
        registerFetchSubPhases(plugins);
        registerSearchExts(plugins);
        registerIntervalsSourceProviders();
        requestCacheKeyDifferentiator = registerRequestCacheKeyDifferentiator(plugins);
        namedWriteables.addAll(SortValue.namedWriteables());
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return namedWriteables;
    }

    public List<NamedXContentRegistry.Entry> getNamedXContents() {
        return namedXContents;
    }

    public ValuesSourceRegistry getValuesSourceRegistry() {
        return valuesSourceRegistry;
    }

    @Nullable
    public CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> getRequestCacheKeyDifferentiator() {
        return requestCacheKeyDifferentiator;
    }

    /**
     * Returns the {@link Highlighter} registry
     */
    public Map<String, Highlighter> getHighlighters() {
        return highlighters;
    }

    private ValuesSourceRegistry registerAggregations(List<SearchPlugin> plugins) {
        ValuesSourceRegistry.Builder builder = new ValuesSourceRegistry.Builder();

        registerAggregation(
            new AggregationSpec(AvgAggregationBuilder.NAME, AvgAggregationBuilder::new, AvgAggregationBuilder.PARSER).addResultReader(
                InternalAvg::new
            ).setAggregatorRegistrar(AvgAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                WeightedAvgAggregationBuilder.NAME,
                WeightedAvgAggregationBuilder::new,
                WeightedAvgAggregationBuilder.PARSER
            ).addResultReader(InternalWeightedAvg::new).setAggregatorRegistrar(WeightedAvgAggregationBuilder::registerUsage),
            builder
        );
        registerAggregation(
            new AggregationSpec(SumAggregationBuilder.NAME, SumAggregationBuilder::new, SumAggregationBuilder.PARSER).addResultReader(
                Sum::new
            ).setAggregatorRegistrar(SumAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(MinAggregationBuilder.NAME, MinAggregationBuilder::new, MinAggregationBuilder.PARSER).addResultReader(
                Min::new
            ).setAggregatorRegistrar(MinAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(MaxAggregationBuilder.NAME, MaxAggregationBuilder::new, MaxAggregationBuilder.PARSER).addResultReader(
                Max::new
            ).setAggregatorRegistrar(MaxAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(StatsAggregationBuilder.NAME, StatsAggregationBuilder::new, StatsAggregationBuilder.PARSER).addResultReader(
                InternalStats::new
            ).setAggregatorRegistrar(StatsAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                ExtendedStatsAggregationBuilder.NAME,
                ExtendedStatsAggregationBuilder::new,
                ExtendedStatsAggregationBuilder.PARSER
            ).addResultReader(InternalExtendedStats::new).setAggregatorRegistrar(ExtendedStatsAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(ValueCountAggregationBuilder.NAME, ValueCountAggregationBuilder::new, ValueCountAggregationBuilder.PARSER)
                .addResultReader(InternalValueCount::new)
                .setAggregatorRegistrar(ValueCountAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                PercentilesAggregationBuilder.NAME,
                PercentilesAggregationBuilder::new,
                PercentilesAggregationBuilder.PARSER
            ).addResultReader(InternalTDigestPercentiles.NAME, InternalTDigestPercentiles::new)
                .addResultReader(InternalHDRPercentiles.NAME, InternalHDRPercentiles::new)
                .setAggregatorRegistrar(PercentilesAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                PercentileRanksAggregationBuilder.NAME,
                PercentileRanksAggregationBuilder::new,
                PercentileRanksAggregationBuilder.PARSER
            ).addResultReader(InternalTDigestPercentileRanks.NAME, InternalTDigestPercentileRanks::new)
                .addResultReader(InternalHDRPercentileRanks.NAME, InternalHDRPercentileRanks::new)
                .setAggregatorRegistrar(PercentileRanksAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                MedianAbsoluteDeviationAggregationBuilder.NAME,
                MedianAbsoluteDeviationAggregationBuilder::new,
                MedianAbsoluteDeviationAggregationBuilder.PARSER
            ).addResultReader(InternalMedianAbsoluteDeviation::new)
                .setAggregatorRegistrar(MedianAbsoluteDeviationAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                CardinalityAggregationBuilder.NAME,
                CardinalityAggregationBuilder::new,
                CardinalityAggregationBuilder.PARSER
            ).addResultReader(InternalCardinality::new).setAggregatorRegistrar(CardinalityAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(GlobalAggregationBuilder.NAME, GlobalAggregationBuilder::new, GlobalAggregationBuilder::parse)
                .addResultReader(InternalGlobal::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(MissingAggregationBuilder.NAME, MissingAggregationBuilder::new, MissingAggregationBuilder.PARSER)
                .addResultReader(InternalMissing::new)
                .setAggregatorRegistrar(MissingAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(FilterAggregationBuilder.NAME, FilterAggregationBuilder::new, FilterAggregationBuilder::parse)
                .addResultReader(InternalFilter::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(FiltersAggregationBuilder.NAME, FiltersAggregationBuilder::new, FiltersAggregationBuilder::parse)
                .addResultReader(InternalFilters::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                RandomSamplerAggregationBuilder.NAME,
                RandomSamplerAggregationBuilder::new,
                RandomSamplerAggregationBuilder.PARSER
            ).addResultReader(InternalRandomSampler.NAME, InternalRandomSampler::new)
                .setAggregatorRegistrar(s -> s.registerUsage(RandomSamplerAggregationBuilder.NAME)),
            builder
        );
        registerAggregation(
            new AggregationSpec(SamplerAggregationBuilder.NAME, SamplerAggregationBuilder::new, SamplerAggregationBuilder::parse)
                .addResultReader(InternalSampler.NAME, InternalSampler::new)
                .addResultReader(UnmappedSampler.NAME, UnmappedSampler::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                DiversifiedAggregationBuilder.NAME,
                DiversifiedAggregationBuilder::new,
                DiversifiedAggregationBuilder.PARSER
            ).setAggregatorRegistrar(DiversifiedAggregationBuilder::registerAggregators)
            /* Reuses result readers from SamplerAggregator*/,
            builder
        );
        registerAggregation(
            new AggregationSpec(TermsAggregationBuilder.NAME, TermsAggregationBuilder::new, TermsAggregationBuilder.PARSER).addResultReader(
                StringTerms.NAME,
                StringTerms::new
            )
                .addResultReader(UnmappedTerms.NAME, UnmappedTerms::new)
                .addResultReader(LongTerms.NAME, LongTerms::new)
                .addResultReader(DoubleTerms.NAME, DoubleTerms::new)
                .setAggregatorRegistrar(TermsAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(RareTermsAggregationBuilder.NAME, RareTermsAggregationBuilder::new, RareTermsAggregationBuilder.PARSER)
                .addResultReader(StringRareTerms.NAME, StringRareTerms::new)
                .addResultReader(UnmappedRareTerms.NAME, UnmappedRareTerms::new)
                .addResultReader(LongRareTerms.NAME, LongRareTerms::new)
                .setAggregatorRegistrar(RareTermsAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                SignificantTermsAggregationBuilder.NAME,
                SignificantTermsAggregationBuilder::new,
                SignificantTermsAggregationBuilder::parse
            ).addResultReader(SignificantStringTerms.NAME, SignificantStringTerms::new)
                .addResultReader(SignificantLongTerms.NAME, SignificantLongTerms::new)
                .addResultReader(UnmappedSignificantTerms.NAME, UnmappedSignificantTerms::new)
                .setAggregatorRegistrar(SignificantTermsAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                SignificantTextAggregationBuilder.NAME,
                SignificantTextAggregationBuilder::new,
                SignificantTextAggregationBuilder::parse
            ),
            builder
        );
        registerAggregation(
            new AggregationSpec(RangeAggregationBuilder.NAME, RangeAggregationBuilder::new, RangeAggregationBuilder.PARSER).addResultReader(
                InternalRange::new
            ).setAggregatorRegistrar(RangeAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(DateRangeAggregationBuilder.NAME, DateRangeAggregationBuilder::new, DateRangeAggregationBuilder.PARSER)
                .addResultReader(InternalDateRange::new)
                .setAggregatorRegistrar(DateRangeAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(IpPrefixAggregationBuilder.NAME, IpPrefixAggregationBuilder::new, IpPrefixAggregationBuilder.PARSER)
                .addResultReader(InternalIpPrefix::new)
                .setAggregatorRegistrar(IpPrefixAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(IpRangeAggregationBuilder.NAME, IpRangeAggregationBuilder::new, IpRangeAggregationBuilder.PARSER)
                .addResultReader(InternalBinaryRange::new)
                .setAggregatorRegistrar(IpRangeAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(HistogramAggregationBuilder.NAME, HistogramAggregationBuilder::new, HistogramAggregationBuilder.PARSER)
                .addResultReader(InternalHistogram::new)
                .setAggregatorRegistrar(HistogramAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                DateHistogramAggregationBuilder.NAME,
                DateHistogramAggregationBuilder::new,
                DateHistogramAggregationBuilder.PARSER
            ).addResultReader(InternalDateHistogram::new).setAggregatorRegistrar(DateHistogramAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                VariableWidthHistogramAggregationBuilder.NAME,
                VariableWidthHistogramAggregationBuilder::new,
                VariableWidthHistogramAggregationBuilder.PARSER
            ).addResultReader(InternalVariableWidthHistogram::new)
                .setAggregatorRegistrar(VariableWidthHistogramAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                GeoDistanceAggregationBuilder.NAME,
                GeoDistanceAggregationBuilder::new,
                GeoDistanceAggregationBuilder::parse
            ).addResultReader(InternalGeoDistance::new).setAggregatorRegistrar(GeoDistanceAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                GeoHashGridAggregationBuilder.NAME,
                GeoHashGridAggregationBuilder::new,
                GeoHashGridAggregationBuilder.PARSER
            ).addResultReader(InternalGeoHashGrid::new).setAggregatorRegistrar(GeoHashGridAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                GeoTileGridAggregationBuilder.NAME,
                GeoTileGridAggregationBuilder::new,
                GeoTileGridAggregationBuilder.PARSER
            ).addResultReader(InternalGeoTileGrid::new).setAggregatorRegistrar(GeoTileGridAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(NestedAggregationBuilder.NAME, NestedAggregationBuilder::new, NestedAggregationBuilder::parse)
                .addResultReader(InternalNested::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                ReverseNestedAggregationBuilder.NAME,
                ReverseNestedAggregationBuilder::new,
                ReverseNestedAggregationBuilder::parse
            ).addResultReader(InternalReverseNested::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(TopHitsAggregationBuilder.NAME, TopHitsAggregationBuilder::new, TopHitsAggregationBuilder::parse)
                .addResultReader(InternalTopHits::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(GeoBoundsAggregationBuilder.NAME, GeoBoundsAggregationBuilder::new, GeoBoundsAggregationBuilder.PARSER)
                .addResultReader(InternalGeoBounds::new)
                .setAggregatorRegistrar(GeoBoundsAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                GeoCentroidAggregationBuilder.NAME,
                GeoCentroidAggregationBuilder::new,
                GeoCentroidAggregationBuilder.PARSER
            ).addResultReader(InternalGeoCentroid::new).setAggregatorRegistrar(GeoCentroidAggregationBuilder::registerAggregators),
            builder
        );
        registerAggregation(
            new AggregationSpec(
                ScriptedMetricAggregationBuilder.NAME,
                ScriptedMetricAggregationBuilder::new,
                ScriptedMetricAggregationBuilder.PARSER
            ).addResultReader(InternalScriptedMetric::new),
            builder
        );
        registerAggregation(
            new AggregationSpec(CompositeAggregationBuilder.NAME, CompositeAggregationBuilder::new, CompositeAggregationBuilder.PARSER)
                .addResultReader(InternalComposite::new)
                .setAggregatorRegistrar(CompositeAggregationBuilder::registerAggregators),
            builder
        );
        if (RestApiVersion.minimumSupported() == RestApiVersion.V_7) {
            registerQuery(
                new QuerySpec<>(
                    CommonTermsQueryBuilder.NAME_V7,
                    (streamInput) -> new CommonTermsQueryBuilder(),
                    CommonTermsQueryBuilder::fromXContent
                )
            );
        }

        registerFromPlugin(plugins, SearchPlugin::getAggregations, (agg) -> this.registerAggregation(agg, builder));

        // after aggs have been registered, see if there are any new VSTypes that need to be linked to core fields
        registerFromPlugin(plugins, SearchPlugin::getAggregationExtentions, (registrar) -> {
            if (registrar != null) {
                registrar.accept(builder);
            }
        });

        return builder.build();
    }

    private void registerAggregation(AggregationSpec spec, ValuesSourceRegistry.Builder builder) {
        namedXContents.add(new NamedXContentRegistry.Entry(BaseAggregationBuilder.class, spec.getName(), (p, c) -> {
            String name = (String) c;
            return spec.getParser().parse(p, name);
        }));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(AggregationBuilder.class, spec.getName().getPreferredName(), spec.getReader())
        );
        for (Map.Entry<String, Writeable.Reader<? extends InternalAggregation>> t : spec.getResultReaders().entrySet()) {
            String writeableName = t.getKey();
            Writeable.Reader<? extends InternalAggregation> internalReader = t.getValue();
            namedWriteables.add(new NamedWriteableRegistry.Entry(InternalAggregation.class, writeableName, internalReader));
        }
        Consumer<ValuesSourceRegistry.Builder> register = spec.getAggregatorRegistrar();
        if (register != null) {
            register.accept(builder);
        } else {
            // Register is typically handling usage registration, but for the older aggregations that don't use register, we
            // have to register usage explicitly here.
            builder.registerUsage(spec.getName().getPreferredName());
        }
    }

    private void registerPipelineAggregations(List<SearchPlugin> plugins) {
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                MaxBucketPipelineAggregationBuilder.NAME,
                MaxBucketPipelineAggregationBuilder::new,
                MaxBucketPipelineAggregationBuilder.PARSER
            )
                // This bucket is used by many pipeline aggreations.
                .addResultReader(InternalBucketMetricValue.NAME, InternalBucketMetricValue::new)
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                MinBucketPipelineAggregationBuilder.NAME,
                MinBucketPipelineAggregationBuilder::new,
                MinBucketPipelineAggregationBuilder.PARSER
            )
            /* Uses InternalBucketMetricValue */
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                AvgBucketPipelineAggregationBuilder.NAME,
                AvgBucketPipelineAggregationBuilder::new,
                AvgBucketPipelineAggregationBuilder.PARSER
            )
                // This bucket is used by many pipeline aggreations.
                .addResultReader(InternalSimpleValue.NAME, InternalSimpleValue::new)
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                SumBucketPipelineAggregationBuilder.NAME,
                SumBucketPipelineAggregationBuilder::new,
                SumBucketPipelineAggregationBuilder.PARSER
            )
            /* Uses InternalSimpleValue */
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                StatsBucketPipelineAggregationBuilder.NAME,
                StatsBucketPipelineAggregationBuilder::new,
                StatsBucketPipelineAggregationBuilder.PARSER
            ).addResultReader(InternalStatsBucket::new)
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                ExtendedStatsBucketPipelineAggregationBuilder.NAME,
                ExtendedStatsBucketPipelineAggregationBuilder::new,
                new ExtendedStatsBucketParser()
            ).addResultReader(InternalExtendedStatsBucket::new)
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                PercentilesBucketPipelineAggregationBuilder.NAME,
                PercentilesBucketPipelineAggregationBuilder::new,
                PercentilesBucketPipelineAggregationBuilder.PARSER
            ).addResultReader(InternalPercentilesBucket::new)
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                CumulativeSumPipelineAggregationBuilder.NAME,
                CumulativeSumPipelineAggregationBuilder::new,
                CumulativeSumPipelineAggregationBuilder.PARSER
            )
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                BucketScriptPipelineAggregationBuilder.NAME,
                BucketScriptPipelineAggregationBuilder::new,
                BucketScriptPipelineAggregationBuilder.PARSER
            )
        );
        registerPipelineAggregation(
            new PipelineAggregationSpec(
                SerialDiffPipelineAggregationBuilder.NAME,
                SerialDiffPipelineAggregationBuilder::new,
                SerialDiffPipelineAggregationBuilder::parse
            )
        );
        if (RestApiVersion.minimumSupported() == RestApiVersion.V_7) {
            registerPipelineAggregation(
                new PipelineAggregationSpec(
                    MovAvgPipelineAggregationBuilder.NAME_V7,
                    MovAvgPipelineAggregationBuilder::new,
                    MovAvgPipelineAggregationBuilder.PARSER
                )
            );
        }

        registerFromPlugin(plugins, SearchPlugin::getPipelineAggregations, this::registerPipelineAggregation);
    }

    private void registerPipelineAggregation(PipelineAggregationSpec spec) {
        namedXContents.add(
            new NamedXContentRegistry.Entry(BaseAggregationBuilder.class, spec.getName(), (p, c) -> spec.getParser().parse(p, (String) c))
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(PipelineAggregationBuilder.class, spec.getName().getPreferredName(), spec.getReader())
        );
        for (Map.Entry<String, Writeable.Reader<? extends InternalAggregation>> resultReader : spec.getResultReaders().entrySet()) {
            namedWriteables.add(
                new NamedWriteableRegistry.Entry(InternalAggregation.class, resultReader.getKey(), resultReader.getValue())
            );
        }
    }

    private void registerRescorers(List<SearchPlugin> plugins) {
        registerRescorer(new RescorerSpec<>(QueryRescorerBuilder.NAME, QueryRescorerBuilder::new, QueryRescorerBuilder::fromXContent));
        registerFromPlugin(plugins, SearchPlugin::getRescorers, this::registerRescorer);
    }

    private void registerRescorer(RescorerSpec<?> spec) {
        namedXContents.add(new NamedXContentRegistry.Entry(RescorerBuilder.class, spec.getName(), (p, c) -> spec.getParser().apply(p)));
        namedWriteables.add(new NamedWriteableRegistry.Entry(RescorerBuilder.class, spec.getName().getPreferredName(), spec.getReader()));
    }

    private void registerSorts() {
        namedWriteables.add(new NamedWriteableRegistry.Entry(SortBuilder.class, GeoDistanceSortBuilder.NAME, GeoDistanceSortBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SortBuilder.class, ScoreSortBuilder.NAME, ScoreSortBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SortBuilder.class, ScriptSortBuilder.NAME, ScriptSortBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SortBuilder.class, FieldSortBuilder.NAME, FieldSortBuilder::new));
    }

    private static <T> void registerFromPlugin(List<SearchPlugin> plugins, Function<SearchPlugin, List<T>> producer, Consumer<T> consumer) {
        for (SearchPlugin plugin : plugins) {
            for (T t : producer.apply(plugin)) {
                consumer.accept(t);
            }
        }
    }

    public static void registerSmoothingModels(List<Entry> namedWriteables) {
        namedWriteables.add(new NamedWriteableRegistry.Entry(SmoothingModel.class, Laplace.NAME, Laplace::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SmoothingModel.class, LinearInterpolation.NAME, LinearInterpolation::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SmoothingModel.class, StupidBackoff.NAME, StupidBackoff::new));
    }

    private void registerSuggesters(List<SearchPlugin> plugins) {
        registerSmoothingModels(namedWriteables);

        registerSuggester(
            new SuggesterSpec<>(
                TermSuggestionBuilder.SUGGESTION_NAME,
                TermSuggestionBuilder::new,
                TermSuggestionBuilder::fromXContent,
                TermSuggestion::new
            )
        );

        registerSuggester(
            new SuggesterSpec<>(
                PhraseSuggestionBuilder.SUGGESTION_NAME,
                PhraseSuggestionBuilder::new,
                PhraseSuggestionBuilder::fromXContent,
                PhraseSuggestion::new
            )
        );

        registerSuggester(
            new SuggesterSpec<>(
                CompletionSuggestionBuilder.SUGGESTION_NAME,
                CompletionSuggestionBuilder::new,
                CompletionSuggestionBuilder::fromXContent,
                CompletionSuggestion::new
            )
        );

        registerFromPlugin(plugins, SearchPlugin::getSuggesters, this::registerSuggester);
    }

    private void registerSuggester(SuggesterSpec<?> suggester) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(SuggestionBuilder.class, suggester.getName().getPreferredName(), suggester.getReader())
        );
        namedXContents.add(new NamedXContentRegistry.Entry(SuggestionBuilder.class, suggester.getName(), suggester.getParser()));

        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                Suggest.Suggestion.class,
                suggester.getName().getPreferredName(),
                suggester.getSuggestionReader()
            )
        );
    }

    private static Map<String, Highlighter> setupHighlighters(Settings settings, List<SearchPlugin> plugins) {
        NamedRegistry<Highlighter> highlighters = new NamedRegistry<>("highlighter");
        highlighters.register("fvh", new FastVectorHighlighter(settings));
        highlighters.register("plain", new PlainHighlighter());
        highlighters.register("unified", new UnifiedHighlighter());
        highlighters.extractAndRegister(plugins, SearchPlugin::getHighlighters);

        return unmodifiableMap(highlighters.getRegistry());
    }

    private void registerScoreFunctions(List<SearchPlugin> plugins) {
        // ScriptScoreFunctionBuilder has it own named writable because of a new script_score query
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ScriptScoreFunctionBuilder.class,
                ScriptScoreFunctionBuilder.NAME,
                ScriptScoreFunctionBuilder::new
            )
        );
        registerScoreFunction(
            new ScoreFunctionSpec<>(
                ScriptScoreFunctionBuilder.NAME,
                ScriptScoreFunctionBuilder::new,
                ScriptScoreFunctionBuilder::fromXContent
            )
        );

        registerScoreFunction(
            new ScoreFunctionSpec<>(GaussDecayFunctionBuilder.NAME, GaussDecayFunctionBuilder::new, GaussDecayFunctionBuilder.PARSER)
        );
        registerScoreFunction(
            new ScoreFunctionSpec<>(LinearDecayFunctionBuilder.NAME, LinearDecayFunctionBuilder::new, LinearDecayFunctionBuilder.PARSER)
        );
        registerScoreFunction(
            new ScoreFunctionSpec<>(
                ExponentialDecayFunctionBuilder.NAME,
                ExponentialDecayFunctionBuilder::new,
                ExponentialDecayFunctionBuilder.PARSER
            )
        );
        registerScoreFunction(
            new ScoreFunctionSpec<>(
                RandomScoreFunctionBuilder.NAME,
                RandomScoreFunctionBuilder::new,
                RandomScoreFunctionBuilder::fromXContent
            )
        );
        registerScoreFunction(
            new ScoreFunctionSpec<>(
                FieldValueFactorFunctionBuilder.NAME,
                FieldValueFactorFunctionBuilder::new,
                FieldValueFactorFunctionBuilder::fromXContent
            )
        );

        // weight doesn't have its own parser, so every function supports it out of the box.
        // Can be a single function too when not associated to any other function, which is why it needs to be registered manually here.
        namedWriteables.add(new NamedWriteableRegistry.Entry(ScoreFunctionBuilder.class, WeightBuilder.NAME, WeightBuilder::new));

        registerFromPlugin(plugins, SearchPlugin::getScoreFunctions, this::registerScoreFunction);
    }

    private void registerScoreFunction(ScoreFunctionSpec<?> scoreFunction) {
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ScoreFunctionBuilder.class,
                scoreFunction.getName().getPreferredName(),
                scoreFunction.getReader()
            )
        );
        // TODO remove funky contexts
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                ScoreFunctionBuilder.class,
                scoreFunction.getName(),
                (XContentParser p, Object c) -> scoreFunction.getParser().fromXContent(p)
            )
        );
    }

    private void registerValueFormats() {
        registerValueFormat(DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN);
        registerValueFormat(DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new);
        registerValueFormat(DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new);
        registerValueFormat(DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH);
        registerValueFormat(DocValueFormat.GEOTILE.getWriteableName(), in -> DocValueFormat.GEOTILE);
        registerValueFormat(DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP);
        registerValueFormat(DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW);
        registerValueFormat(DocValueFormat.BINARY.getWriteableName(), in -> DocValueFormat.BINARY);
        registerValueFormat(DocValueFormat.UNSIGNED_LONG_SHIFTED.getWriteableName(), in -> DocValueFormat.UNSIGNED_LONG_SHIFTED);
        registerValueFormat(DocValueFormat.TIME_SERIES_ID.getWriteableName(), in -> DocValueFormat.TIME_SERIES_ID);
    }

    /**
     * Register a new ValueFormat.
     */
    private void registerValueFormat(String name, Writeable.Reader<? extends DocValueFormat> reader) {
        namedWriteables.add(new NamedWriteableRegistry.Entry(DocValueFormat.class, name, reader));
    }

    private void registerSignificanceHeuristics(List<SearchPlugin> plugins) {
        registerSignificanceHeuristic(new SignificanceHeuristicSpec<>(ChiSquare.NAME, ChiSquare::new, ChiSquare.PARSER));
        registerSignificanceHeuristic(new SignificanceHeuristicSpec<>(GND.NAME, GND::new, GND.PARSER));
        registerSignificanceHeuristic(new SignificanceHeuristicSpec<>(JLHScore.NAME, JLHScore::new, JLHScore.PARSER));
        registerSignificanceHeuristic(
            new SignificanceHeuristicSpec<>(MutualInformation.NAME, MutualInformation::new, MutualInformation.PARSER)
        );
        registerSignificanceHeuristic(new SignificanceHeuristicSpec<>(PercentageScore.NAME, PercentageScore::new, PercentageScore.PARSER));
        registerSignificanceHeuristic(new SignificanceHeuristicSpec<>(ScriptHeuristic.NAME, ScriptHeuristic::new, ScriptHeuristic.PARSER));

        registerFromPlugin(plugins, SearchPlugin::getSignificanceHeuristics, this::registerSignificanceHeuristic);
    }

    private <T extends SignificanceHeuristic> void registerSignificanceHeuristic(SignificanceHeuristicSpec<?> spec) {
        namedXContents.add(
            new NamedXContentRegistry.Entry(SignificanceHeuristic.class, spec.getName(), p -> spec.getParser().apply(p, null))
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(SignificanceHeuristic.class, spec.getName().getPreferredName(), spec.getReader())
        );
    }

    private void registerQueryVectorBuilders(List<SearchPlugin> plugins) {
        registerFromPlugin(plugins, SearchPlugin::getQueryVectorBuilders, this::registerQueryVectorBuilder);
    }

    private <T extends QueryVectorBuilder> void registerQueryVectorBuilder(QueryVectorBuilderSpec<?> spec) {
        namedXContents.add(new NamedXContentRegistry.Entry(QueryVectorBuilder.class, spec.getName(), p -> spec.getParser().apply(p, null)));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(QueryVectorBuilder.class, spec.getName().getPreferredName(), spec.getReader())
        );
    }

    private void registerFetchSubPhases(List<SearchPlugin> plugins) {
        registerFetchSubPhase(new ExplainPhase());
        registerFetchSubPhase(new StoredFieldsPhase());
        registerFetchSubPhase(new FetchDocValuesPhase());
        registerFetchSubPhase(new ScriptFieldsPhase());
        registerFetchSubPhase(new FetchSourcePhase());
        registerFetchSubPhase(new FetchFieldsPhase());
        registerFetchSubPhase(new FetchVersionPhase());
        registerFetchSubPhase(new SeqNoPrimaryTermPhase());
        registerFetchSubPhase(new MatchedQueriesPhase());
        registerFetchSubPhase(new HighlightPhase(highlighters));
        registerFetchSubPhase(new FetchScorePhase());

        FetchPhaseConstructionContext context = new FetchPhaseConstructionContext(highlighters);
        registerFromPlugin(plugins, p -> p.getFetchSubPhases(context), this::registerFetchSubPhase);
    }

    private void registerSearchExts(List<SearchPlugin> plugins) {
        registerFromPlugin(plugins, SearchPlugin::getSearchExts, this::registerSearchExt);
    }

    private void registerSearchExt(SearchExtSpec<?> spec) {
        namedXContents.add(new NamedXContentRegistry.Entry(SearchExtBuilder.class, spec.getName(), spec.getParser()));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SearchExtBuilder.class, spec.getName().getPreferredName(), spec.getReader()));
    }

    private void registerFetchSubPhase(FetchSubPhase subPhase) {
        Class<?> subPhaseClass = subPhase.getClass();
        if (fetchSubPhases.stream().anyMatch(p -> p.getClass().equals(subPhaseClass))) {
            throw new IllegalArgumentException("FetchSubPhase [" + subPhaseClass + "] already registered");
        }
        fetchSubPhases.add(requireNonNull(subPhase, "FetchSubPhase must not be null"));
    }

    private void registerQueryParsers(List<SearchPlugin> plugins) {
        registerQuery(new QuerySpec<>(MatchQueryBuilder.NAME, MatchQueryBuilder::new, MatchQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchPhraseQueryBuilder.NAME, MatchPhraseQueryBuilder::new, MatchPhraseQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(
                MatchPhrasePrefixQueryBuilder.NAME,
                MatchPhrasePrefixQueryBuilder::new,
                MatchPhrasePrefixQueryBuilder::fromXContent
            )
        );
        registerQuery(new QuerySpec<>(MultiMatchQueryBuilder.NAME, MultiMatchQueryBuilder::new, MultiMatchQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(CombinedFieldsQueryBuilder.NAME, CombinedFieldsQueryBuilder::new, CombinedFieldsQueryBuilder::fromXContent)
        );
        registerQuery(new QuerySpec<>(NestedQueryBuilder.NAME, NestedQueryBuilder::new, NestedQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(DisMaxQueryBuilder.NAME, DisMaxQueryBuilder::new, DisMaxQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(IdsQueryBuilder.NAME, IdsQueryBuilder::new, IdsQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new, MatchAllQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(QueryStringQueryBuilder.NAME, QueryStringQueryBuilder::new, QueryStringQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(BoostingQueryBuilder.NAME, BoostingQueryBuilder::new, BoostingQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(BoolQueryBuilder.NAME, BoolQueryBuilder::new, BoolQueryBuilder::fromXContent));
        AbstractQueryBuilder.setMaxNestedDepth(INDICES_MAX_NESTED_DEPTH_SETTING.get(settings));
        registerQuery(new QuerySpec<>(TermQueryBuilder.NAME, TermQueryBuilder::new, TermQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(TermsQueryBuilder.NAME, TermsQueryBuilder::new, TermsQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(FuzzyQueryBuilder.NAME, FuzzyQueryBuilder::new, FuzzyQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(RegexpQueryBuilder.NAME, RegexpQueryBuilder::new, RegexpQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(RangeQueryBuilder.NAME, RangeQueryBuilder::new, RangeQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(PrefixQueryBuilder.NAME, PrefixQueryBuilder::new, PrefixQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(WildcardQueryBuilder.NAME, WildcardQueryBuilder::new, WildcardQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(ConstantScoreQueryBuilder.NAME, ConstantScoreQueryBuilder::new, ConstantScoreQueryBuilder::fromXContent)
        );
        registerQuery(new QuerySpec<>(SpanTermQueryBuilder.NAME, SpanTermQueryBuilder::new, SpanTermQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanNotQueryBuilder.NAME, SpanNotQueryBuilder::new, SpanNotQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanWithinQueryBuilder.NAME, SpanWithinQueryBuilder::new, SpanWithinQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(SpanContainingQueryBuilder.NAME, SpanContainingQueryBuilder::new, SpanContainingQueryBuilder::fromXContent)
        );
        registerQuery(
            new QuerySpec<>(
                FieldMaskingSpanQueryBuilder.NAME,
                FieldMaskingSpanQueryBuilder::new,
                FieldMaskingSpanQueryBuilder::fromXContent
            )
        );
        registerQuery(new QuerySpec<>(SpanFirstQueryBuilder.NAME, SpanFirstQueryBuilder::new, SpanFirstQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanNearQueryBuilder.NAME, SpanNearQueryBuilder::new, SpanNearQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanGapQueryBuilder.NAME, SpanGapQueryBuilder::new, SpanGapQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(SpanOrQueryBuilder.NAME, SpanOrQueryBuilder::new, SpanOrQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(MoreLikeThisQueryBuilder.NAME, MoreLikeThisQueryBuilder::new, MoreLikeThisQueryBuilder::fromXContent)
        );
        registerQuery(new QuerySpec<>(WrapperQueryBuilder.NAME, WrapperQueryBuilder::new, WrapperQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(SpanMultiTermQueryBuilder.NAME, SpanMultiTermQueryBuilder::new, SpanMultiTermQueryBuilder::fromXContent)
        );
        registerQuery(
            new QuerySpec<>(FunctionScoreQueryBuilder.NAME, FunctionScoreQueryBuilder::new, FunctionScoreQueryBuilder::fromXContent)
        );
        registerQuery(new QuerySpec<>(ScriptScoreQueryBuilder.NAME, ScriptScoreQueryBuilder::new, ScriptScoreQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(SimpleQueryStringBuilder.NAME, SimpleQueryStringBuilder::new, SimpleQueryStringBuilder::fromXContent)
        );
        registerQuery(new QuerySpec<>(ScriptQueryBuilder.NAME, ScriptQueryBuilder::new, ScriptQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(GeoDistanceQueryBuilder.NAME, GeoDistanceQueryBuilder::new, GeoDistanceQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(GeoBoundingBoxQueryBuilder.NAME, GeoBoundingBoxQueryBuilder::new, GeoBoundingBoxQueryBuilder::fromXContent)
        );
        registerQuery(
            new QuerySpec<>(
                (new ParseField(GeoPolygonQueryBuilder.NAME).withAllDeprecated(GeoPolygonQueryBuilder.GEO_POLYGON_DEPRECATION_MSG)),
                GeoPolygonQueryBuilder::new,
                GeoPolygonQueryBuilder::fromXContent
            )
        );
        registerQuery(new QuerySpec<>(ExistsQueryBuilder.NAME, ExistsQueryBuilder::new, ExistsQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(MatchNoneQueryBuilder.NAME, MatchNoneQueryBuilder::new, MatchNoneQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(TermsSetQueryBuilder.NAME, TermsSetQueryBuilder::new, TermsSetQueryBuilder::fromXContent));
        registerQuery(new QuerySpec<>(IntervalQueryBuilder.NAME, IntervalQueryBuilder::new, IntervalQueryBuilder::fromXContent));
        registerQuery(
            new QuerySpec<>(DistanceFeatureQueryBuilder.NAME, DistanceFeatureQueryBuilder::new, DistanceFeatureQueryBuilder::fromXContent)
        );
        registerQuery(
            new QuerySpec<>(MatchBoolPrefixQueryBuilder.NAME, MatchBoolPrefixQueryBuilder::new, MatchBoolPrefixQueryBuilder::fromXContent)
        );
        registerQuery(new QuerySpec<>(GeoShapeQueryBuilder.NAME, GeoShapeQueryBuilder::new, GeoShapeQueryBuilder::fromXContent));

        registerQuery(new QuerySpec<>(KnnVectorQueryBuilder.NAME, KnnVectorQueryBuilder::new, parser -> {
            throw new IllegalArgumentException("[knn] queries cannot be provided directly, use the [knn] body parameter instead");
        }));

        registerQuery(new QuerySpec<>(KnnScoreDocQueryBuilder.NAME, KnnScoreDocQueryBuilder::new, parser -> {
            throw new IllegalArgumentException("[score_doc] queries cannot be provided directly");
        }));

        registerFromPlugin(plugins, SearchPlugin::getQueries, this::registerQuery);

        if (RestApiVersion.minimumSupported() == RestApiVersion.V_7) {
            registerQuery(new QuerySpec<>(TypeQueryV7Builder.NAME_V7, TypeQueryV7Builder::new, TypeQueryV7Builder::fromXContent));
        }
    }

    private void registerIntervalsSourceProviders() {
        namedWriteables.addAll(getIntervalsSourceProviderNamedWritables());
    }

    private static CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> registerRequestCacheKeyDifferentiator(
        List<SearchPlugin> plugins
    ) {
        CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> differentiator = null;
        for (SearchPlugin plugin : plugins) {
            final CheckedBiConsumer<ShardSearchRequest, StreamOutput, IOException> d = plugin.getRequestCacheKeyDifferentiator();
            if (d != null) {
                if (differentiator == null) {
                    differentiator = d;
                } else {
                    throw new IllegalArgumentException("Cannot have more than one plugin providing a request cache key differentiator");
                }
            }
        }
        return differentiator;
    }

    public static List<NamedWriteableRegistry.Entry> getIntervalsSourceProviderNamedWritables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                IntervalsSourceProvider.class,
                IntervalsSourceProvider.Match.NAME,
                IntervalsSourceProvider.Match::new
            ),
            new NamedWriteableRegistry.Entry(
                IntervalsSourceProvider.class,
                IntervalsSourceProvider.Combine.NAME,
                IntervalsSourceProvider.Combine::new
            ),
            new NamedWriteableRegistry.Entry(
                IntervalsSourceProvider.class,
                IntervalsSourceProvider.Disjunction.NAME,
                IntervalsSourceProvider.Disjunction::new
            ),
            new NamedWriteableRegistry.Entry(
                IntervalsSourceProvider.class,
                IntervalsSourceProvider.Prefix.NAME,
                IntervalsSourceProvider.Prefix::new
            ),
            new NamedWriteableRegistry.Entry(
                IntervalsSourceProvider.class,
                IntervalsSourceProvider.Wildcard.NAME,
                IntervalsSourceProvider.Wildcard::new
            ),
            new NamedWriteableRegistry.Entry(
                IntervalsSourceProvider.class,
                IntervalsSourceProvider.Fuzzy.NAME,
                IntervalsSourceProvider.Fuzzy::new
            )
        );
    }

    private void registerQuery(QuerySpec<?> spec) {
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, spec.getName().getPreferredName(), spec.getReader()));
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                spec.getName(),
                (p, c) -> spec.getParser().fromXContent(p),
                spec.getName().getForRestApiVersion()
            )
        );
    }

    public FetchPhase getFetchPhase() {
        return new FetchPhase(fetchSubPhases);
    }

}
