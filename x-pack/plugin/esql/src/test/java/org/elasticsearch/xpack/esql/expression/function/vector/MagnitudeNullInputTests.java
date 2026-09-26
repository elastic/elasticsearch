/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.junit.Before;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MagnitudeNullInputTests extends ESTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("v_magnitude available in snapshot", EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled());
    }

    public void testSingleNullVectorKeepsOnePosition() {
        assertNullPage(1);
    }

    public void testAllNullPageKeepsPositionCount() {
        assertNullPage(3);
    }

    private void assertNullPage(int positions) {
        FieldAttribute vector = new FieldAttribute(
            Source.EMPTY,
            "v",
            new EsField("v", DataType.DENSE_VECTOR, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Magnitude magnitude = new Magnitude(Source.EMPTY, vector);
        Layout layout = new Layout.Builder().append(vector).build();
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FoldContext.small(), magnitude, layout).get(driverContext);
        try (FloatBlock input = nullFloatBlock(blockFactory, positions); Block result = evaluator.eval(new Page(input))) {
            assertThat(result.getPositionCount(), equalTo(positions));
            for (int p = 0; p < positions; p++) {
                assertThat(result.isNull(p), is(true));
            }
        } finally {
            evaluator.close();
        }
    }

    private static FloatBlock nullFloatBlock(BlockFactory blockFactory, int positions) {
        try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                builder.appendNull();
            }
            return builder.build();
        }
    }
}
