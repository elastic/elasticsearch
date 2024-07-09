/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
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
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.countInnerBucket;
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
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, () -> false, aggs, b -> {});
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
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, () -> false, agg, b -> {});
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

    @SuppressWarnings("this-escape")
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(getNamedWriteables());

    public static InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        try (AggregatorReducer reducer = aggregations.get(0).getReducer(reduceContext, aggregations.size())) {
            for (InternalAggregation aggregation : aggregations) {
                reducer.accept(aggregation);
            }
            return reducer.get();
        }
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

    private static void collectSubBuilderNames(Map<String, Object> names, InternalAggregation result) {
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
            AggregationReduceContext context = new AggregationReduceContext.ForPartial(
                bigArrays,
                mockScriptService,
                () -> false,
                inputs.builder(),
                b -> {}
            );
            @SuppressWarnings("unchecked")
            T reduced = (T) reduce(toPartialReduce, context);
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
        @SuppressWarnings("unchecked")
        T reduced = (T) reduce(toReduce, context);
        doAssertReducedMultiBucketConsumer(reduced, bucketConsumer);
        assertReduced(reduced, inputs.toReduce());
        if (supportsSampling()) {
            SamplingContext randomContext = new SamplingContext(
                randomDoubleBetween(1e-8, 0.1, false),
                randomInt(),
                randomBoolean() ? null : randomInt()
            );
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
            return TransportVersions.ZERO;
        }
    }
}
