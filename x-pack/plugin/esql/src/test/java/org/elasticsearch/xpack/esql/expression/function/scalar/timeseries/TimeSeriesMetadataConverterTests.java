/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Loads {@code _timeseries} through {@link TimeSeriesMetadataConverter}. {@link ComputeTestCase} checks after each test
 * that the loaded block and the converter's scratch are released: the reader hands {@code convert} the loaded block.
 */
public class TimeSeriesMetadataConverterTests extends ComputeTestCase {
    public void testConvertsToALabelContainer() {
        try (BytesRefBlock converted = convert("{\"pod\":\"a\",\"cluster\":\"\",\"k8s\":{\"node\":[\"n1\",\"n2\"]}}", null, "{}")) {
            BytesRef scratch = new BytesRef();
            BytesRef first = converted.getBytesRef(converted.getFirstValueIndex(0), scratch);
            assertThat(first, equalTo(TimeSeriesMetadataEncoding.encode(Map.of("k8s.node", List.of("n1", "n2"), "pod", List.of("a")))));
            assertThat(TimeSeriesMetadataEncoding.toJson(first).utf8ToString(), equalTo("{\"k8s.node\":[\"n1\",\"n2\"],\"pod\":\"a\"}"));
            assertTrue(converted.isNull(1));
            assertThat(converted.getBytesRef(converted.getFirstValueIndex(2), scratch), equalTo(TimeSeriesMetadataEncoding.EMPTY));
        }
    }

    /** A series has a row per time bucket: its label set converts once, and its rows point at it. */
    public void testDictionaryEncodesEachLabelSetOnce() {
        String a = "{\"pod\":\"a\",\"job\":\"api\"}";
        String b = "{\"job\":\"api\",\"pod\":\"b\"}";
        try (BytesRefBlock converted = convert(a, b, a, null, b, a)) {
            OrdinalBytesRefBlock ordinals = converted.asOrdinals();
            assertThat(ordinals, notNullValue());
            BytesRefVector dictionary = ordinals.getDictionaryVector();
            assertThat(dictionary.getPositionCount(), equalTo(2));
            BytesRef scratch = new BytesRef();
            for (int p : new int[] { 0, 2, 5 }) {
                assertThat(
                    converted.getBytesRef(converted.getFirstValueIndex(p), scratch),
                    equalTo(TimeSeriesMetadataEncoding.encode(Map.of("job", List.of("api"), "pod", List.of("a"))))
                );
            }
            assertTrue(converted.isNull(3));
        }
    }

    private BytesRefBlock convert(String... sources) {
        BlockFactory blockFactory = blockFactory();
        DriverContext context = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            ValuesSourceReaderOperator.ConverterEvaluator converter = TimeSeriesMetadataConverter.INSTANCE.build(context);
            BytesRefBlock.Builder loaded = blockFactory.newBytesRefBlockBuilder(sources.length)
        ) {
            for (String source : sources) {
                if (source == null) {
                    loaded.appendNull();
                } else {
                    loaded.appendBytesRef(new BytesRef(source));
                }
            }
            return (BytesRefBlock) converter.convert(loaded.build());
        }
    }
}
