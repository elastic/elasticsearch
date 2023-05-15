/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoTileGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedVariableWidthHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.VariableWidthHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringRareTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.ParsedMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.ParsedWeightedAvg;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.pipeline.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.countInnerBucket;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Implementors of this test case should be aware that the aggregation under test needs to be registered
 * in the test's namedWriteableRegistry.  Core aggregations are registered already, but non-core
 * aggs should override {@link InternalAggregationTestCase#registerPlugin()} so that the NamedWriteables
 * can be extracted from the AggregatorSpecs in the plugin (as well as any other custom NamedWriteables)
 */
public abstract class InternalAggregationTestCase<T extends InternalAggregation> extends AbstractNamedWriteableTestCase<T> {
    /**
     * Builds an {@link AggregationReduceContext} that is valid but empty.
     */
    public static AggregationReduceContext.Builder emptyReduceContextBuilder() {
        return emptyReduceContextBuilder(AggregatorFactories.builder());
    }

    /**
     * Builds an {@link AggregationReduceContext} that is valid and nearly
     * empty <strong>except</strong> that it contains the provided builders.
     */
    public static AggregationReduceContext.Builder emptyReduceContextBuilder(AggregatorFactories.Builder aggs) {
        return new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, () -> false, aggs);
            }

            @Override
            public AggregationReduceContext forFinalReduction() {
                return new AggregationReduceContext.ForFinal(BigArrays.NON_RECYCLING_INSTANCE, null, () -> false, aggs, b -> {});
            }
        };
    }

    /**
     * Builds an {@link AggregationReduceContext} to reduce the provided
     * aggregation.
     */
    public static AggregationReduceContext.Builder mockReduceContext(AggregationBuilder agg) {
        return new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, () -> false, agg);
            }

            @Override
            public AggregationReduceContext forFinalReduction() {
                return new AggregationReduceContext.ForFinal(
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null,
                    () -> false,
                    agg,
                    b -> {},
                    PipelineTree.EMPTY
                );
            }
        };
    }

    public static final int DEFAULT_MAX_BUCKETS = 100000;
    protected static final double TOLERANCE = 1e-10;

    private static final Comparator<InternalAggregation> INTERNAL_AGG_COMPARATOR = (agg1, agg2) -> {
        if (agg1.canLeadReduction() == agg2.canLeadReduction()) {
            return 0;
        } else if (agg1.canLeadReduction() && agg2.canLeadReduction() == false) {
            return -1;
        } else {
            return 1;
        }
    };

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(getNamedWriteables());

    private final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(getNamedXContents());

    private static final List<NamedXContentRegistry.Entry> namedXContents;
    static {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MedianAbsoluteDeviationAggregationBuilder.NAME, (p, c) -> ParsedMedianAbsoluteDeviation.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(WeightedAvgAggregationBuilder.NAME, (p, c) -> ParsedWeightedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoBoundsAggregationBuilder.NAME, (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        map.put(VariableWidthHistogramAggregationBuilder.NAME, (p, c) -> ParsedVariableWidthHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(LongRareTerms.NAME, (p, c) -> ParsedLongRareTerms.fromXContent(p, (String) c));
        map.put(StringRareTerms.NAME, (p, c) -> ParsedStringRareTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(GeoHashGridAggregationBuilder.NAME, (p, c) -> ParsedGeoHashGrid.fromXContent(p, (String) c));
        map.put(GeoTileGridAggregationBuilder.NAME, (p, c) -> ParsedGeoTileGrid.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));

        namedXContents = map.entrySet()
            .stream()
            .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
            .collect(Collectors.toList());
    }

    public static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        return namedXContents;
    }

    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return namedXContents;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    /**
     * Implementors can override this if they want to provide a custom list of namedWriteables.  If the implementor
     * _just_ wants to register in namedWriteables provided by a plugin, prefer overriding
     * {@link InternalAggregationTestCase#registerPlugin()} instead because that route handles the automatic
     * conversion of AggSpecs into namedWriteables.
     */
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        SearchPlugin plugin = registerPlugin();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, plugin == null ? emptyList() : List.of(plugin));
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(searchModule.getNamedWriteables());

        // Modules/plugins may have extra namedwriteables that are not added by agg specs
        if (plugin != null) {
            entries.addAll(((Plugin) plugin).getNamedWriteables());
        }

        return entries;
    }

    /**
     * If a test needs to register additional aggregation specs for namedWriteable, etc, this method
     * can be overridden by the implementor.
     */
    protected SearchPlugin registerPlugin() {
        return null;
    }

    protected abstract T createTestInstance(String name, Map<String, Object> metadata);

    /** Return an instance on an unmapped field. */
    protected T createUnmappedInstance(String name, Map<String, Object> metadata) {
        // For most impls, we use the same instance in the unmapped case and in the mapped case
        return createTestInstance(name, metadata);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final Class<T> categoryClass() {
        return (Class<T>) InternalAggregation.class;
    }

    /**
     * Generate a list of inputs to reduce. Defaults to calling
     * {@link #createTestInstance(String)} and
     * {@link #createUnmappedInstance(String)} and {@link #mockBuilder(List)}
     * but should be overridden if it isn't realistic to reduce test
     * instances against mocked builders.
     */
    protected BuilderAndToReduce<T> randomResultsToReduce(String name, int size) {
        List<T> inputs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            T t = randomBoolean() ? createUnmappedInstance(name) : createTestInstance(name);
            inputs.add(t);
        }
        return new BuilderAndToReduce<>(mockBuilder(inputs), inputs);
    }

    protected final AggregationBuilder mockBuilder(List<? extends InternalAggregation> results) {
        Map<String, Object> subNames = new HashMap<>();
        results.forEach(a -> collectSubBuilderNames(subNames, a));
        return mockBuilder(results.get(0).getName(), subNames);
    }

    private AggregationBuilder mockBuilder(String name, Map<String, Object> subNames) {
        AggregationBuilder b = new MockAggregationBuilder(name);
        for (Map.Entry<String, Object> s : subNames.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> subSubNames = (Map<String, Object>) s.getValue();
            b.subAggregation(mockBuilder(s.getKey(), subSubNames));
        }
        return b;
    }

    private void collectSubBuilderNames(Map<String, Object> names, InternalAggregation result) {
        result.forEachBucket(ia -> {
            for (InternalAggregation a : ia.copyResults()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> sub = (Map<String, Object>) names.computeIfAbsent(a.getName(), k -> new HashMap<String, Object>());
                collectSubBuilderNames(sub, a);
            }
        });
    }

    public record BuilderAndToReduce<T>(AggregationBuilder builder, List<T> toReduce) {}

    /**
     * Does this aggregation support reductions when the internal buckets are not in-order
     */
    protected boolean supportsOutOfOrderReduce() {
        return true;
    }

    public void testReduceRandom() throws IOException {
        String name = randomAlphaOfLength(5);
        int size = between(1, 200);
        BuilderAndToReduce<T> inputs = randomResultsToReduce(name, size);
        assertThat(inputs.toReduce(), hasSize(size));
        List<InternalAggregation> toReduce = new ArrayList<>();
        toReduce.addAll(inputs.toReduce());
        ScriptService mockScriptService = mockScriptService();
        MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        if (randomBoolean() && toReduce.size() > 1) {
            // sometimes do a partial reduce
            if (supportsOutOfOrderReduce()) {
                Collections.shuffle(toReduce, random());
            }
            int r = randomIntBetween(1, toReduce.size());
            List<InternalAggregation> toPartialReduce = toReduce.subList(0, r);
            // Sort aggs so that unmapped come last. This mimicks the behavior of InternalAggregations.reduce()
            toPartialReduce.sort(INTERNAL_AGG_COMPARATOR);
            AggregationReduceContext context = new AggregationReduceContext.ForPartial(
                bigArrays,
                mockScriptService,
                () -> false,
                inputs.builder()
            );
            @SuppressWarnings("unchecked")
            T reduced = (T) toPartialReduce.get(0).reduce(toPartialReduce, context);
            int initialBucketCount = 0;
            for (InternalAggregation internalAggregation : toPartialReduce) {
                initialBucketCount += countInnerBucket(internalAggregation);
            }
            int reducedBucketCount = countInnerBucket(reduced);
            // check that non final reduction never adds buckets
            assertThat(reducedBucketCount, lessThanOrEqualTo(initialBucketCount));
            /*
             * Sometimes serializing and deserializing the partially reduced
             * result to simulate the compaction that we attempt after a
             * partial reduce. And to simulate cross cluster search.
             */
            if (randomBoolean()) {
                reduced = copyNamedWriteable(reduced, getNamedWriteableRegistry(), categoryClass());
            }
            toReduce = new ArrayList<>(toReduce.subList(r, toReduce.size()));
            toReduce.add(reduced);
        }
        MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        AggregationReduceContext context = new AggregationReduceContext.ForFinal(
            bigArrays,
            mockScriptService,
            () -> false,
            inputs.builder(),
            bucketConsumer,
            PipelineTree.EMPTY
        );
        // Sort aggs so that unmapped come last. This mimicks the behavior of InternalAggregations.reduce()
        inputs.toReduce().sort(INTERNAL_AGG_COMPARATOR);
        @SuppressWarnings("unchecked")
        T reduced = (T) inputs.toReduce().get(0).reduce(toReduce, context);
        doAssertReducedMultiBucketConsumer(reduced, bucketConsumer);
        assertReduced(reduced, inputs.toReduce());
        if (supportsSampling()) {
            SamplingContext randomContext = new SamplingContext(randomDoubleBetween(1e-8, 0.1, false), randomInt());
            @SuppressWarnings("unchecked")
            T sampled = (T) reduced.finalizeSampling(randomContext);
            assertSampled(sampled, reduced, randomContext);
        }
    }

    protected void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        InternalAggregationTestCase.assertMultiBucketConsumer(agg, bucketConsumer);
    }

    /**
     * overwrite in tests that need it
     */
    protected ScriptService mockScriptService() {
        return null;
    }

    protected abstract void assertReduced(T reduced, List<T> inputs);

    protected void assertSampled(T sampled, T reduced, SamplingContext samplingContext) {
        throw new UnsupportedOperationException("aggregation supports sampling but does not implement assertSampled");
    }

    @Override
    public final T createTestInstance() {
        return createTestInstance(randomAlphaOfLength(5));
    }

    protected boolean supportsSampling() {
        return false;
    }

    public final Map<String, Object> createTestMetadata() {
        Map<String, Object> metadata = null;
        if (randomBoolean()) {
            metadata = new HashMap<>();
            int metadataCount = between(0, 10);
            while (metadata.size() < metadataCount) {
                metadata.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
        }
        return metadata;
    }

    private T createTestInstance(String name) {
        return createTestInstance(name, createTestMetadata());
    }

    /** Return an instance on an unmapped field. */
    protected final T createUnmappedInstance(String name) {
        Map<String, Object> metadata = new HashMap<>();
        int metadataCount = randomBoolean() ? 0 : between(1, 10);
        while (metadata.size() < metadataCount) {
            metadata.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return createUnmappedInstance(name, metadata);
    }

    public T createTestInstanceForXContent() {
        return createTestInstance();
    }

    public final void testFromXContent() throws IOException {
        final T aggregation = createTestInstanceForXContent();
        final ParsedAggregation parsedAggregation = parseAndAssert(aggregation, randomBoolean(), false);
        assertFromXContent(aggregation, parsedAggregation);
    }

    public final void testFromXContentWithRandomFields() throws IOException {
        final T aggregation = createTestInstanceForXContent();
        final ParsedAggregation parsedAggregation = parseAndAssert(aggregation, randomBoolean(), true);
        assertFromXContent(aggregation, parsedAggregation);
    }

    protected abstract void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) throws IOException;

    /**
     * Calls {@link ToXContent#toXContent} on many threads and verifies that
     * they produce the same result. Async search sometimes does this to
     * aggregation responses and, in general, we think it's reasonable for
     * everything that can convert itself to json to be able to do so
     * concurrently.
     */
    public final void testConcurrentToXContent() throws IOException, InterruptedException, ExecutionException {
        T testInstance = createTestInstanceForXContent();
        ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesRef firstTimeBytes = toXContent(testInstance, xContentType, params, humanReadable).toBytesRef();

        /*
         * 500 rounds seems to consistently reproduce the issue on Nik's
         * laptop. Larger numbers are going to be slower but more likely
         * to reproduce the issue.
         */
        int rounds = scaledRandomIntBetween(300, 5000);
        concurrentTest(() -> {
            try {
                for (int r = 0; r < rounds; r++) {
                    assertEquals(firstTimeBytes, toXContent(testInstance, xContentType, params, humanReadable).toBytesRef());
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    protected <P extends ParsedAggregation> P parseAndAssert(
        final InternalAggregation aggregation,
        final boolean shuffled,
        final boolean addRandomFields
    ) throws IOException {

        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        final XContentType xContentType = randomFrom(XContentType.values());
        final boolean humanReadable = randomBoolean();

        final BytesReference originalBytes;
        try {
            if (shuffled) {
                originalBytes = toShuffledXContent(aggregation, xContentType, params, humanReadable);
            } else {
                originalBytes = toXContent(aggregation, xContentType, params, humanReadable);
            }
        } catch (IOException e) {
            throw new IOException("error converting " + aggregation, e);
        }
        BytesReference mutated;
        if (addRandomFields) {
            /*
             * - we don't add to the root object because it should only contain
             * the named aggregation to test - we don't want to insert into the
             * "meta" object, because we pass on everything we find there
             *
             * - we don't want to directly insert anything random into "buckets"
             * objects, they are used with "keyed" aggregations and contain
             * named bucket objects. Any new named object on this level should
             * also be a bucket and be parsed as such.
             *
             * we also exclude top_hits that contain SearchHits, as all unknown fields
             * on a root level of SearchHit are interpreted as meta-fields and will be kept.
             */
            Predicate<String> basicExcludes = path -> path.isEmpty()
                || path.endsWith(Aggregation.CommonFields.META.getPreferredName())
                || path.endsWith(Aggregation.CommonFields.BUCKETS.getPreferredName())
                || path.contains("top_hits");
            Predicate<String> excludes = basicExcludes.or(excludePathsFromXContentInsertion());
            mutated = insertRandomFields(xContentType, originalBytes, excludes, random());
        } else {
            mutated = originalBytes;
        }

        SetOnce<Aggregation> parsedAggregation = new SetOnce<>();
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class, parsedAggregation::set);

            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());

            Aggregation agg = parsedAggregation.get();
            assertEquals(aggregation.getName(), agg.getName());
            assertEquals(aggregation.getMetadata(), agg.getMetadata());

            assertTrue(agg instanceof ParsedAggregation);
            assertEquals(aggregation.getType(), agg.getType());

            BytesReference parsedBytes = toXContent(agg, xContentType, params, humanReadable);
            assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);

            return (P) agg;
        }

    }

    /**
     * Overwrite this in your test if other than the basic xContent paths should be excluded during insertion of random fields
     */
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> false;
    }

    /**
     * A random {@link DocValueFormat} that can be used in aggregations which
     * compute numbers.
     */
    public static DocValueFormat randomNumericDocValueFormat() {
        final List<Supplier<DocValueFormat>> formats = new ArrayList<>(3);
        formats.add(() -> DocValueFormat.RAW);
        formats.add(() -> new DocValueFormat.Decimal(randomFrom("###.##", "###,###.##")));
        return randomFrom(formats).get();
    }

    /**
     * A random {@link DocValueFormat} that can be used in aggregations which
     * compute dates.
     */
    public static DocValueFormat randomDateDocValueFormat() {
        DocValueFormat.DateTime format = new DocValueFormat.DateTime(
            DateFormatter.forPattern(randomDateFormatterPattern()),
            randomZone(),
            randomFrom(Resolution.values())
        );
        if (randomBoolean()) {
            return DocValueFormat.enableFormatSortValues(format);
        }
        return format;
    }

    public static void assertMultiBucketConsumer(Aggregation agg, MultiBucketConsumer bucketConsumer) {
        assertMultiBucketConsumer(countInnerBucket(agg), bucketConsumer);
    }

    private static void assertMultiBucketConsumer(int innerBucketCount, MultiBucketConsumer bucketConsumer) {
        assertThat(bucketConsumer.getCount(), equalTo(innerBucketCount));
    }

    private class MockAggregationBuilder extends AbstractAggregationBuilder<MockAggregationBuilder> {
        MockAggregationBuilder(String name) {
            super(name);
        }

        @Override
        public String getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subfactoriesBuilder)
            throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BucketCardinality bucketCardinality() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.ZERO;
        }
    }
}
