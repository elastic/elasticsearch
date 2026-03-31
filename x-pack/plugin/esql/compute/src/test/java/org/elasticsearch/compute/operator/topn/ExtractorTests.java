/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ExtractorTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        List<Object[]> cases = new ArrayList<>();
        for (ElementType e : ElementType.values()) {
            boolean supportsNull = true;
            switch (e) {
                case UNKNOWN -> {
                    supportsNull = false;
                }
                case COMPOSITE -> {
                    // TODO: add later
                    supportsNull = false;
                }
                case AGGREGATE_METRIC_DOUBLE -> {
                    cases.add(
                        valueTestCase(
                            "regular aggregate_metric_double",
                            e,
                            TopNEncoder.DEFAULT_UNSORTABLE,
                            false,
                            () -> randomAggregateMetricDouble(true)
                        )
                    );
                    cases.add(
                        valueTestCase(
                            "aggregate_metric_double with nulls",
                            e,
                            TopNEncoder.DEFAULT_UNSORTABLE,
                            false,
                            () -> randomAggregateMetricDouble(false)
                        )
                    );
                }
                case LONG_RANGE -> {
                    cases.add(
                        valueTestCase("date_range with nulls", e, TopNEncoder.DEFAULT_UNSORTABLE, false, () -> randomDateRange(true))
                    );
                    cases.add(
                        valueTestCase("date_range with nulls", e, TopNEncoder.DEFAULT_UNSORTABLE, false, () -> randomDateRange(false))
                    );
                }
                case FLOAT -> {
                    supportsNull = false;
                }
                case BYTES_REF -> {
                    cases.add(valueTestCase("single alpha", e, TopNEncoder.UTF8, true, () -> randomAlphaOfLength(5)));
                    cases.add(
                        valueTestCase("many alpha", e, TopNEncoder.UTF8, true, () -> randomList(2, 10, () -> randomAlphaOfLength(5)))
                    );
                    cases.add(valueTestCase("single utf8", e, TopNEncoder.UTF8, true, () -> randomRealisticUnicodeOfLength(10)));
                    cases.add(
                        valueTestCase(
                            "many utf8",
                            e,
                            TopNEncoder.UTF8,
                            true,
                            () -> randomList(2, 10, () -> randomRealisticUnicodeOfLength(10))
                        )
                    );
                    cases.add(
                        valueTestCase("single version", e, TopNEncoder.VERSION, true, () -> TopNEncoderTests.randomVersion().toBytesRef())
                    );
                    cases.add(
                        valueTestCase(
                            "many version",
                            e,
                            TopNEncoder.VERSION,
                            true,
                            () -> randomList(2, 10, () -> TopNEncoderTests.randomVersion().toBytesRef())
                        )
                    );
                    cases.add(
                        valueTestCase(
                            "single IP",
                            e,
                            TopNEncoder.IP,
                            true,
                            () -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())))
                        )
                    );
                    cases.add(
                        valueTestCase(
                            "many IP",
                            e,
                            TopNEncoder.IP,
                            true,
                            () -> randomList(2, 10, () -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))))
                        )
                    );
                    cases.add(valueTestCase("single point", e, TopNEncoder.DEFAULT_UNSORTABLE, false, TopNEncoderTests::randomPointAsWKB));
                    cases.add(
                        valueTestCase(
                            "many points",
                            e,
                            TopNEncoder.DEFAULT_UNSORTABLE,
                            false,
                            () -> randomList(2, 10, TopNEncoderTests::randomPointAsWKB)
                        )
                    );
                }
                case DOC -> {
                    supportsNull = false;
                    cases.add(
                        new Object[] {
                            new TestCase(
                                "doc",
                                e,
                                new DocVectorEncoder(AlwaysReferencedIndexedByShardId.INSTANCE),
                                false,
                                () -> new DocVector(
                                    AlwaysReferencedIndexedByShardId.INSTANCE,
                                    // Shard ID should be small and non-negative.
                                    blockFactory.newConstantIntBlockWith(randomIntBetween(0, 255), 1).asVector(),
                                    blockFactory.newConstantIntBlockWith(randomInt(), 1).asVector(),
                                    blockFactory.newConstantIntBlockWith(randomInt(), 1).asVector(),
                                    docVectorConfig()
                                ).asBlock(),
                                b -> {
                                    DocVector v = (DocVector) b.asVector();
                                    return new DocVector(
                                        AlwaysReferencedIndexedByShardId.INSTANCE,
                                        v.shards(),
                                        v.segments(),
                                        v.docs(),
                                        DocVector.config().mayContainDuplicates()
                                    ).asBlock();
                                }
                            ) }
                    );
                }
                case TDIGEST, EXPONENTIAL_HISTOGRAM ->
                    // multi values are not supported
                    cases.add(valueTestCase("single " + e, e, TopNEncoder.DEFAULT_UNSORTABLE, false, () -> BlockTestUtils.randomValue(e)));
                case NULL -> {
                }
                default -> {
                    cases.add(valueTestCase("single " + e, e, TopNEncoder.DEFAULT_UNSORTABLE, false, () -> BlockTestUtils.randomValue(e)));
                    cases.add(
                        valueTestCase(
                            "many " + e,
                            e,
                            TopNEncoder.DEFAULT_UNSORTABLE,
                            false,
                            () -> randomList(2, 10, () -> BlockTestUtils.randomValue(e))
                        )
                    );
                }
            }
            if (supportsNull) {
                cases.add(valueTestCase("null " + e, e, TopNEncoder.DEFAULT_UNSORTABLE, false, () -> null));
            }
        }
        return cases;
    }

    static Object[] valueTestCase(String name, ElementType type, TopNEncoder encoder, boolean sortable, Supplier<Object> value) {
        return new Object[] {
            new TestCase(
                name,
                type,
                encoder,
                sortable,
                () -> BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), Arrays.asList(value.get()))[0],
                Function.identity()
            ) };
    }

    static class TestCase {
        private final String name;
        private final ElementType type;
        private final TopNEncoder encoder;
        private final boolean sortable;
        private final Supplier<Block> value;
        private final Function<Block, Block> expected;

        TestCase(
            String name,
            ElementType type,
            TopNEncoder encoder,
            boolean sortable,
            Supplier<Block> value,
            Function<Block, Block> expected
        ) {
            this.name = name;
            this.type = type;
            this.encoder = encoder;
            this.sortable = sortable;
            this.value = value;
            this.expected = expected;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private final TestCase testCase;

    public ExtractorTests(TestCase testCase) {
        this.testCase = testCase;
    }

    static BreakingBytesRefBuilder nonBreakingBytesRefBuilder() {
        return new BreakingBytesRefBuilder(new NoopCircuitBreaker(CircuitBreaker.REQUEST), "topn");
    }

    public void testNotInKey() {
        Block value = testCase.value.get();

        BreakingBytesRefBuilder valuesBuilder = nonBreakingBytesRefBuilder();
        ValueExtractor.extractorFor(testCase.type, testCase.encoder.toUnsortable(), false, value).writeValue(valuesBuilder, 0);
        assertThat(valuesBuilder.length(), greaterThan(0));

        ResultBuilder result = ResultBuilder.resultBuilderFor(
            TestBlockFactory.getNonBreakingInstance(),
            testCase.type,
            testCase.encoder.toUnsortable(),
            false,
            1
        );
        BytesRef values = valuesBuilder.bytesRefView();
        result.decodeValue(values);
        assertThat(values.length, equalTo(0));

        Block resultBlock = result.build();
        assertThat(resultBlock, equalTo(testCase.expected.apply(value)));
    }

    public void testInKey() {
        if (testCase.sortable == false) {
            return;
        }
        Block value = testCase.value.get();

        BreakingBytesRefBuilder keysBuilder = nonBreakingBytesRefBuilder();
        boolean asc = randomBoolean();
        KeyExtractor.extractorFor(testCase.type, testCase.encoder, asc, randomByte(), randomByte(), value).writeKey(keysBuilder, 0);
        assertThat(keysBuilder.length(), greaterThan(0));

        BreakingBytesRefBuilder valuesBuilder = nonBreakingBytesRefBuilder();
        ValueExtractor.extractorFor(testCase.type, testCase.encoder.toUnsortable(), true, value).writeValue(valuesBuilder, 0);
        assertThat(valuesBuilder.length(), greaterThan(0));

        ResultBuilder result = ResultBuilder.resultBuilderFor(
            TestBlockFactory.getNonBreakingInstance(),
            testCase.type,
            testCase.encoder,
            true,
            1
        );
        BytesRef keys = keysBuilder.bytesRefView();
        if (testCase.type == ElementType.NULL) {
            assertThat(keys.length, equalTo(1));
        } else {
            // Skip the non-null byte
            keys.offset++;
            keys.length--;
            result.decodeKey(keys, asc);
            assertThat(keys.length, equalTo(0));
        }
        BytesRef values = valuesBuilder.bytesRefView();
        result.decodeValue(values);
        assertThat(values.length, equalTo(0));

        assertThat(result.build(), equalTo(testCase.expected.apply(value)));
    }

    public static AggregateMetricDoubleLiteral randomAggregateMetricDouble(boolean allMetrics) {
        if (allMetrics) {
            return new AggregateMetricDoubleLiteral(randomDouble(), randomDouble(), randomDouble(), randomInt());
        }
        return new AggregateMetricDoubleLiteral(
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomDouble() : null,
            randomBoolean() ? randomInt() : null
        );
    }

    private static LongRangeBlockBuilder.LongRange randomDateRange(boolean haveNulls) {
        var from = randomMillisUpToYear9999();
        var to = randomLongBetween(from + 1, MAX_MILLIS_BEFORE_9999);
        return haveNulls
            ? new LongRangeBlockBuilder.LongRange(randomBoolean() ? from : null, randomBoolean() ? to : null)
            : new LongRangeBlockBuilder.LongRange(from, to);
    }

    private static DocVector.Config docVectorConfig() {
        DocVector.Config config = DocVector.config();
        if (randomBoolean()) {
            config.singleSegmentNonDecreasing(randomBoolean());
        }
        return config;
    }
}
