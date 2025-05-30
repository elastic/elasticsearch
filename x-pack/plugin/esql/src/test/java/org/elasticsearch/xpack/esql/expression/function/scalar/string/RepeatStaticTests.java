/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * These tests create rows that are 1MB in size. Test classes
 * which extend AbstractScalarFunctionTestCase rerun test cases with
 * many randomized inputs. Unfortunately, tests are run with
 * limited memory, and instantiating many copies of these
 * tests with large rows causes out of memory.
 */
public class RepeatStaticTests extends ESTestCase {

    public void testAlmostTooBig() {
        String str = randomAlphaOfLength(1);
        int number = (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE;
        String repeated = process(str, number);
        assertThat(repeated, equalTo(str.repeat(number)));
    }

    public void testTooBig() {
        String str = randomAlphaOfLength(1);
        int number = (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE + 1;
        String repeated = process(str, number);
        assertNull(repeated);
        assertWarnings(
            "Line -1:-1: java.lang.IllegalArgumentException: Creating repeated strings with more than [1048576] bytes is not supported",
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded."
        );
    }

    public String process(String str, int number) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Repeat(Source.EMPTY, field("string", DataType.KEYWORD), field("number", DataType.INTEGER))
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(str), number)));
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
