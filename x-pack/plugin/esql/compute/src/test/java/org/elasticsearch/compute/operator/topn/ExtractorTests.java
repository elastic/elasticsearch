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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ExtractorTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> cases = new ArrayList<>();
        for (ElementType e : ElementType.values()) {
            switch (e) {
                case UNKNOWN -> {
                }
                case BYTES_REF -> {
                    cases.add(valueTestCase("single alpha", e, TopNEncoder.UTF8, () -> randomAlphaOfLength(5)));
                    cases.add(valueTestCase("many alpha", e, TopNEncoder.UTF8, () -> randomList(2, 10, () -> randomAlphaOfLength(5))));
                    cases.add(valueTestCase("single utf8", e, TopNEncoder.UTF8, () -> randomRealisticUnicodeOfLength(10)));
                    cases.add(
                        valueTestCase("many utf8", e, TopNEncoder.UTF8, () -> randomList(2, 10, () -> randomRealisticUnicodeOfLength(10)))
                    );
                    cases.add(valueTestCase("single version", e, TopNEncoder.VERSION, () -> TopNEncoderTests.randomVersion().toBytesRef()));
                    cases.add(
                        valueTestCase(
                            "many version",
                            e,
                            TopNEncoder.VERSION,
                            () -> randomList(2, 10, () -> TopNEncoderTests.randomVersion().toBytesRef())
                        )
                    );
                    cases.add(
                        valueTestCase(
                            "single IP",
                            e,
                            TopNEncoder.IP,
                            () -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())))
                        )
                    );
                    cases.add(
                        valueTestCase(
                            "many IP",
                            e,
                            TopNEncoder.IP,
                            () -> randomList(2, 10, () -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))))
                        )
                    );
                }
                case DOC -> cases.add(
                    new Object[] {
                        new TestCase(
                            "doc",
                            e,
                            TopNEncoder.DEFAULT_UNSORTABLE,
                            () -> new DocVector(
                                IntBlock.newConstantBlockWith(randomInt(), 1).asVector(),
                                IntBlock.newConstantBlockWith(randomInt(), 1).asVector(),
                                IntBlock.newConstantBlockWith(randomInt(), 1).asVector(),
                                randomBoolean() ? null : randomBoolean()
                            ).asBlock()
                        ) }
                );
                case NULL -> cases.add(valueTestCase("null", e, TopNEncoder.DEFAULT_UNSORTABLE, () -> null));
                default -> {
                    cases.add(valueTestCase("single " + e, e, TopNEncoder.DEFAULT_UNSORTABLE, () -> BlockTestUtils.randomValue(e)));
                    cases.add(
                        valueTestCase(
                            "many " + e,
                            e,
                            TopNEncoder.DEFAULT_UNSORTABLE,
                            () -> randomList(2, 10, () -> BlockTestUtils.randomValue(e))
                        )
                    );
                }
            }
        }
        return cases;
    }

    static Object[] valueTestCase(String name, ElementType type, TopNEncoder encoder, Supplier<Object> value) {
        return new Object[] {
            new TestCase(
                name,
                type,
                encoder,
                () -> BlockUtils.fromListRow(BlockFactory.getNonBreakingInstance(), Arrays.asList(value.get()))[0]
            ) };
    }

    static class TestCase {
        private final String name;
        private final ElementType type;
        private final TopNEncoder encoder;
        private final Supplier<Block> value;

        TestCase(String name, ElementType type, TopNEncoder encoder, Supplier<Block> value) {
            this.name = name;
            this.type = type;
            this.encoder = encoder;
            this.value = value;
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
            BlockFactory.getNonBreakingInstance(),
            testCase.type,
            testCase.encoder.toUnsortable(),
            false,
            1
        );
        BytesRef values = valuesBuilder.bytesRefView();
        result.decodeValue(values);
        assertThat(values.length, equalTo(0));

        assertThat(result.build(), equalTo(value));
    }

    public void testInKey() {
        assumeFalse("can't sort on _doc", testCase.type == ElementType.DOC);
        Block value = testCase.value.get();

        BreakingBytesRefBuilder keysBuilder = nonBreakingBytesRefBuilder();
        KeyExtractor.extractorFor(testCase.type, testCase.encoder.toSortable(), randomBoolean(), randomByte(), randomByte(), value)
            .writeKey(keysBuilder, 0);
        assertThat(keysBuilder.length(), greaterThan(0));

        BreakingBytesRefBuilder valuesBuilder = nonBreakingBytesRefBuilder();
        ValueExtractor.extractorFor(testCase.type, testCase.encoder.toUnsortable(), true, value).writeValue(valuesBuilder, 0);
        assertThat(valuesBuilder.length(), greaterThan(0));

        ResultBuilder result = ResultBuilder.resultBuilderFor(
            BlockFactory.getNonBreakingInstance(),
            testCase.type,
            testCase.encoder.toUnsortable(),
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
            result.decodeKey(keys);
            assertThat(keys.length, equalTo(0));
        }
        BytesRef values = valuesBuilder.bytesRefView();
        result.decodeValue(values);
        assertThat(values.length, equalTo(0));

        assertThat(result.build(), equalTo(value));
    }
}
