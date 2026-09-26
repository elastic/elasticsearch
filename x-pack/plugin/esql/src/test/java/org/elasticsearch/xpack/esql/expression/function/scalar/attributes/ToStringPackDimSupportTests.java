/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.attributes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

/** The response renderer preserves source compatibility without changing packed-record semantics or ownership. */
public class ToStringPackDimSupportTests extends ComputeTestCase {
    public void testNestedSourceRenderingPreservesInputAndRepeatedRows() throws Exception {
        try (var builder = blockFactory().newPackDimBlockBuilder(4)) {
            BytesRef[] names = {
                new BytesRef("count"),
                new BytesRef("enabled"),
                new BytesRef("labels.empty"),
                new BytesRef("labels.missing"),
                new BytesRef("labels.pod"),
                new BytesRef("values") };
            BytesRef[] values = {
                new BytesRef("42"),
                new BytesRef("true"),
                new BytesRef("\"\""),
                new BytesRef("null"),
                new BytesRef("\"one\""),
                new BytesRef("[1,null,1]") };
            builder.append(names, values);
            builder.appendNull();
            builder.append(new BytesRef[0], new BytesRef[0]);
            builder.append(names, values);
            try (var input = builder.build(); var output = render(input)) {
                assertEquals(4, output.getPositionCount());
                Object expected = Map.of(
                    "count",
                    42,
                    "enabled",
                    true,
                    "labels",
                    Map.of("empty", "", "pod", "one"),
                    "values",
                    java.util.Arrays.asList(1, null, 1)
                );
                assertEquals(expected, PackDimValueCodec.decode((BytesRef) BlockUtils.toJavaObject(output, 0)));
                assertTrue(output.isNull(1));
                assertEquals(new BytesRef("{}"), BlockUtils.toJavaObject(output, 2));
                assertEquals(BlockUtils.toJavaObject(output, 0), BlockUtils.toJavaObject(output, 3));
                assertEquals(2, input.asOrdinalPackDim().getDictionarySize());
                var original = input.getPackDim(input.getFirstValueIndex(0), new PackDimValue());
                assertEquals(6, original.size());
                assertEquals(new BytesRef("null"), original.get(new BytesRef("labels.missing"), new BytesRef()));
            }
        }
    }

    public void testOutputSurvivesInputRelease() {
        Block output;
        try (var builder = blockFactory().newPackDimBlockBuilder(1)) {
            builder.append(new BytesRef[] { new BytesRef("labels.pod") }, new BytesRef[] { new BytesRef("\"one\"") });
            try (var input = builder.build()) {
                output = render(input);
            }
        }
        try (output) {
            assertEquals(new BytesRef("{\"labels\":{\"pod\":\"one\"}}"), BlockUtils.toJavaObject(output, 0));
        }
    }

    public void testNullAndZeroPositionBlocks() {
        for (int positions : new int[] { 0, 3 }) {
            try (var input = blockFactory().newConstantNullBlock(positions); var output = render(input)) {
                assertEquals(positions, output.getPositionCount());
                for (int p = 0; p < positions; p++)
                    assertTrue(output.isNull(p));
            }
        }
    }

    public void testOlderTransportRejected() throws Exception {
        var input = new ReferenceAttribute(Source.EMPTY, "packed", DataType.PACK_DIM);
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.minimumCompatible());
            var planOutput = new PlanStreamOutput(out, TEST_CFG);
            var error = expectThrows(
                QlIllegalArgumentException.class,
                () -> new ToString(Source.EMPTY, input, TEST_CFG).writeTo(planOutput)
            );
            assertTrue(error.getMessage().contains("doesn't understand data type [PACK_DIM]"));
        }
    }

    public void testAcceptsStringInput() {
        var input = new ReferenceAttribute(Source.EMPTY, "json", DataType.KEYWORD);
        assertTrue(new ToString(Source.EMPTY, input, TEST_CFG).typeResolved().resolved());
    }

    public void testPackedInputTypeAndSerialization() {
        var input = new ReferenceAttribute(Source.EMPTY, "packed", DataType.PACK_DIM);
        var expression = new ToString(Source.EMPTY, input, TEST_CFG);
        assertTrue(expression.typeResolved().resolved());
        assertEquals(DataType.KEYWORD, expression.dataType());
        SerializationTestUtils.assertSerialization(expression);
    }

    private Block render(Block input) {
        var attribute = new ReferenceAttribute(Source.EMPTY, "packed", DataType.PACK_DIM);
        var expression = new ToString(Source.EMPTY, attribute, TEST_CFG);
        var layout = new Layout.Builder().append(List.of(attribute)).build();
        var factory = blockFactory();
        var context = new DriverContext(factory.bigArrays(), factory, null);
        try (var evaluator = EvalMapper.toEvaluator(FoldContext.small(), expression, layout).get(context)) {
            return evaluator.eval(new Page(input));
        }
    }
}
