/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsTests;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class InternalAggregationsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables()
    );

    public void testReduceEmptyAggs() {
        List<InternalAggregations> aggs = Collections.emptyList();
        AggregationReduceContext.Builder builder = InternalAggregationTestCase.emptyReduceContextBuilder();
        AggregationReduceContext reduceContext = randomBoolean() ? builder.forFinalReduction() : builder.forPartialReduction();
        assertNull(InternalAggregations.reduce(aggs, reduceContext));
    }

    public void testReduceIncludesBuilder() {
        AggregatorFactories.Builder builders = new AggregatorFactories.Builder();
        builders.addAggregator(
            new FiltersAggregationBuilder(
                "f1",
                new KeyedFilter("f1k1", new TermQueryBuilder("f1", "k1")),
                new KeyedFilter("f1k2", new TermQueryBuilder("f1", "k2"))
            ).subAggregation(
                new FiltersAggregationBuilder(
                    "f2",
                    new KeyedFilter("f2k1", new TermQueryBuilder("f2", "k1")),
                    new KeyedFilter("f2k2", new TermQueryBuilder("f2", "k2"))
                )
            )
        );
        AtomicLong f1Reduced = new AtomicLong();
        AtomicLong f2Reduced = new AtomicLong();

        InternalAggregations s1 = toReduce(f1Reduced, f2Reduced, 3, 3, 1, 1, 1, 1);
        InternalAggregations s2 = toReduce(f1Reduced, f2Reduced, 4, 9, 1, 2, 1, 0);

        InternalAggregations reduced = InternalAggregations.topLevelReduce(
            List.of(s1, s2),
            new AggregationReduceContext.ForFinal(null, null, () -> false, builders, b -> {})
        );
        assertThat(f1Reduced.get(), equalTo(1L));
        assertThat(f2Reduced.get(), equalTo(2L));
        assertThat(reduced.asList(), equalTo(reduced(7, 12, 2, 3, 2, 1).asList()));
    }

    InternalAggregations toReduce(AtomicLong f1Reduced, AtomicLong f2Reduced, int k1, int k2, int k1k1, int k1k2, int k2k1, int k2k2) {
        class InternalFiltersForF2 extends InternalFilters {
            InternalFiltersForF2(
                String name,
                List<InternalBucket> buckets,
                boolean keyed,
                boolean keyedBucket,
                Map<String, Object> metadata
            ) {
                super(name, buckets, keyed, keyedBucket, metadata);
            }

            @Override
            protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
                assertThat(reduceContext.builder().getName(), equalTo("f2"));
                assertThat(reduceContext.builder(), instanceOf(FiltersAggregationBuilder.class));
                f2Reduced.incrementAndGet();
                return super.getLeaderReducer(reduceContext, size);
            }
        }
        class InternalFiltersForF1 extends InternalFilters {
            InternalFiltersForF1(
                String name,
                List<InternalBucket> buckets,
                boolean keyed,
                boolean keyedBucket,
                Map<String, Object> metadata
            ) {
                super(name, buckets, keyed, keyedBucket, metadata);
            }

            @Override
            protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
                assertThat(reduceContext.builder().getName(), equalTo("f1"));
                assertThat(reduceContext.builder(), instanceOf(FiltersAggregationBuilder.class));
                f1Reduced.incrementAndGet();
                return super.getLeaderReducer(reduceContext, size);
            }
        }
        return InternalAggregations.from(
            List.of(
                new InternalFiltersForF1(
                    "f1",
                    List.of(
                        new InternalFilters.InternalBucket(
                            "f1k1",
                            k1,
                            InternalAggregations.from(
                                List.of(
                                    new InternalFiltersForF2(
                                        "f2",
                                        List.of(
                                            new InternalFilters.InternalBucket("f2k1", k1k1, InternalAggregations.EMPTY),
                                            new InternalFilters.InternalBucket("f2k2", k1k2, InternalAggregations.EMPTY)
                                        ),
                                        true,
                                        true,
                                        null
                                    )
                                )
                            )
                        ),
                        new InternalFilters.InternalBucket(
                            "f1k2",
                            k2,
                            InternalAggregations.from(
                                List.of(
                                    new InternalFiltersForF2(
                                        "f2",
                                        List.of(
                                            new InternalFilters.InternalBucket("f2k1", k2k1, InternalAggregations.EMPTY),
                                            new InternalFilters.InternalBucket("f2k2", k2k2, InternalAggregations.EMPTY)
                                        ),
                                        true,
                                        true,
                                        null
                                    )
                                )
                            )
                        )
                    ),
                    true,
                    true,
                    null
                )
            )
        );
    }

    InternalAggregations reduced(int k1, int k2, int k1k1, int k1k2, int k2k1, int k2k2) {
        return InternalAggregations.from(
            List.of(
                new InternalFilters(
                    "f1",
                    List.of(
                        new InternalFilters.InternalBucket(
                            "f1k1",
                            k1,
                            InternalAggregations.from(
                                List.of(
                                    new InternalFilters(
                                        "f2",
                                        List.of(
                                            new InternalFilters.InternalBucket("f2k1", k1k1, InternalAggregations.EMPTY),
                                            new InternalFilters.InternalBucket("f2k2", k1k2, InternalAggregations.EMPTY)
                                        ),
                                        true,
                                        true,
                                        null
                                    )
                                )
                            )
                        ),
                        new InternalFilters.InternalBucket(
                            "f1k2",
                            k2,
                            InternalAggregations.from(
                                List.of(
                                    new InternalFilters(
                                        "f2",
                                        List.of(
                                            new InternalFilters.InternalBucket("f2k1", k2k1, InternalAggregations.EMPTY),
                                            new InternalFilters.InternalBucket("f2k2", k2k2, InternalAggregations.EMPTY)
                                        ),
                                        true,
                                        true,
                                        null
                                    )
                                )
                            )
                        )
                    ),
                    true,
                    true,
                    null
                )
            )
        );
    }

    public void testNonFinalReduceTopLevelPipelineAggs() {
        InternalAggregation terms = new StringTerms(
            "name",
            BucketOrder.key(true),
            BucketOrder.key(true),
            10,
            1,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            10,
            Collections.emptyList(),
            0L
        );
        List<InternalAggregations> aggs = singletonList(InternalAggregations.from(Collections.singletonList(terms)));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, maxBucketReduceContext().forPartialReduction());
        assertEquals(1, reducedAggs.asList().size());
    }

    public void testFinalReduceTopLevelPipelineAggs() {
        InternalAggregation terms = new StringTerms(
            "name",
            BucketOrder.key(true),
            BucketOrder.key(true),
            10,
            1,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            10,
            Collections.emptyList(),
            0L
        );

        InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(terms));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(List.of(aggs), maxBucketReduceContext().forFinalReduction());
        assertEquals(2, reducedAggs.asList().size());
    }

    private AggregationReduceContext.Builder maxBucketReduceContext() {
        AggregationBuilder aggBuilder = new TermsAggregationBuilder("name");
        PipelineAggregationBuilder pipelineBuilder = new MaxBucketPipelineAggregationBuilder("test", "name");
        return InternalAggregationTestCase.emptyReduceContextBuilder(
            AggregatorFactories.builder().addAggregator(aggBuilder).addPipelineAggregator(pipelineBuilder)
        );
    }

    public static InternalAggregations createTestInstance() throws Exception {
        List<InternalAggregation> aggsList = new ArrayList<>();
        if (randomBoolean()) {
            StringTermsTests stringTermsTests = new StringTermsTests();
            stringTermsTests.init();
            stringTermsTests.setUp();
            aggsList.add(stringTermsTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalDateHistogramTests dateHistogramTests = new InternalDateHistogramTests();
            dateHistogramTests.setUp();
            aggsList.add(dateHistogramTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalSimpleValueTests simpleValueTests = new InternalSimpleValueTests();
            aggsList.add(simpleValueTests.createTestInstance());
        }
        return InternalAggregations.from(aggsList);
    }

    public void testSerialization() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        writeToAndReadFrom(aggregations, TransportVersion.current(), 0);
    }

    public void testSerializedSize() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        assertThat(
            DelayableWriteable.getSerializedSize(aggregations),
            equalTo((long) serialize(aggregations, TransportVersion.current()).length)
        );
    }

    private void writeToAndReadFrom(InternalAggregations aggregations, TransportVersion version, int iteration) throws IOException {
        BytesRef serializedAggs = serialize(aggregations, version);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(serializedAggs.bytes), registry)) {
            in.setTransportVersion(version);
            InternalAggregations deserialized = InternalAggregations.readFrom(in);
            assertEquals(aggregations.asList(), deserialized.asList());
            if (iteration < 2) {
                writeToAndReadFrom(deserialized, version, iteration + 1);
            }
        }
    }

    private BytesRef serialize(InternalAggregations aggs, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            aggs.writeTo(out);
            return out.bytes().toBytesRef();
        }
    }
}
