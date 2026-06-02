/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LongRangeExtractorTests extends ESTestCase {

    public void testMultiValueFollowedByNull() {
        try (LongRangeBlockBuilder builder = TestBlockFactory.getNonBreakingInstance().newLongRangeBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendLongRange(10, 20);
            builder.appendLongRange(30, 40);
            builder.endPositionEntry();
            builder.appendNull();

            try (var value = builder.build()) {
                var valuesBuilder = ExtractorTests.nonBreakingBytesRefBuilder();
                ValueExtractor.extractorFor(ElementType.LONG_RANGE, TopNEncoder.DEFAULT_UNSORTABLE, false, value)
                    .writeValue(valuesBuilder, 0);

                try (
                    ResultBuilder result = ResultBuilder.resultBuilderFor(
                        TestBlockFactory.getNonBreakingInstance(),
                        ElementType.LONG_RANGE,
                        TopNEncoder.DEFAULT_UNSORTABLE,
                        false,
                        1
                    )
                ) {
                    BytesRef values = valuesBuilder.bytesRefView();
                    result.decodeValue(values);
                    assertThat(values.length, equalTo(0));

                    try (Block resultBlock = result.build(); Block expectedBlock = value.filter(false, 0)) {
                        assertThat(resultBlock, equalTo(expectedBlock));
                    }
                }
            }
        }
    }
}
