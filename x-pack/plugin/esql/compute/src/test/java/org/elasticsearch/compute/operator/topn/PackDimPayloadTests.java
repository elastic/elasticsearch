/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.test.ComputeTestCase;

/** Sparse records must survive sorting on other columns without becoming sort keys themselves. */
public class PackDimPayloadTests extends ComputeTestCase {
    public void testRoundTrip() {
        var factory = blockFactory();
        try (
            var builder = factory.newPackDimBlockBuilder(4);
            var encoded = new BreakingBytesRefBuilder(factory.breaker(), "attribute-set-test");
            var result = ResultBuilder.resultBuilderFor(factory, ElementType.PACK_DIM, TopNEncoder.DEFAULT_UNSORTABLE, false, 4)
        ) {
            builder.append(
                new BytesRef[] { new BytesRef("a"), new BytesRef("z") },
                new BytesRef[] { new BytesRef("opaque"), new BytesRef() }
            );
            builder.appendNull();
            builder.append(new BytesRef[0], new BytesRef[0]);
            builder.append(
                new BytesRef[] { new BytesRef("a"), new BytesRef("z") },
                new BytesRef[] { new BytesRef("opaque"), new BytesRef() }
            );
            try (var input = builder.build()) {
                var extractor = ValueExtractor.extractorFor(ElementType.PACK_DIM, TopNEncoder.DEFAULT_UNSORTABLE, false, input);
                for (int p = 0; p < 4; p++)
                    extractor.writeValue(encoded, p);
                BytesRef bytes = encoded.bytesRefView();
                for (int p = 0; p < 4; p++)
                    result.decodeValue(bytes);
                assertEquals(0, bytes.length);
                try (var output = (PackDimBlock) result.build()) {
                    assertEquals(4, output.getPositionCount());
                    assertTrue(output.isNull(1));
                    assertFalse(output.isNull(2));
                    assertEquals(0, output.getPackDim(output.getFirstValueIndex(2), new PackDimValue()).size());
                    for (int p : new int[] { 0, 3 }) {
                        assertEquals(
                            new BytesRef("opaque"),
                            output.getPackDim(output.getFirstValueIndex(p), new PackDimValue()).get(new BytesRef("a"), new BytesRef())
                        );
                    }
                }
            }
        }
    }
}
