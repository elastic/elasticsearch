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
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Loads {@code _timeseries} through {@link TimeSeriesMetadataConverter}. {@link ComputeTestCase} checks after each test
 * that the loaded block and the converter's scratch are released: the reader hands {@code convert} the loaded block.
 */
public class TimeSeriesMetadataConverterTests extends ComputeTestCase {
    public void testConvertsToTheCanonicalForm() {
        BlockFactory blockFactory = blockFactory();
        DriverContext context = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        try (
            ValuesSourceReaderOperator.ConverterEvaluator converter = TimeSeriesMetadataConverter.INSTANCE.build(context);
            BytesRefBlock.Builder loaded = blockFactory.newBytesRefBlockBuilder(3)
        ) {
            loaded.appendBytesRef(new BytesRef("{\"pod\":\"a\",\"cluster\":\"\",\"k8s\":{\"node\":[\"n1\",\"n2\"]}}"));
            loaded.appendNull();
            loaded.appendBytesRef(new BytesRef("{}"));
            try (BytesRefBlock converted = (BytesRefBlock) converter.convert(loaded.build())) {
                BytesRef scratch = new BytesRef();
                assertThat(
                    converted.getBytesRef(converted.getFirstValueIndex(0), scratch).utf8ToString(),
                    equalTo("{\"k8s.node\":[\"n1\",\"n2\"],\"pod\":\"a\"}")
                );
                assertTrue(converted.isNull(1));
                assertThat(converted.getBytesRef(converted.getFirstValueIndex(2), scratch).utf8ToString(), equalTo("{}"));
            }
        }
    }
}
