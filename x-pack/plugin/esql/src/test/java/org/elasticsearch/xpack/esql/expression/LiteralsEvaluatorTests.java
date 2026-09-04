/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LiteralsEvaluatorTests extends ComputeTestCase {
    public record TestCase(Literal lit, String expectedToString) {
        @Override
        public String toString() {
            return "lit=" + lit + "/" + lit.dataType() + " expectedToString=" + expectedToString;
        }
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static List<Object[]> params() {
        List<TestCase> params = new ArrayList<>();
        for (DataType type : DataType.values()) {
            if (type.isNumeric() == false) {
                continue;
            }
            params.add(new TestCase(new Literal(Source.EMPTY, 1, type), "LiteralsEvaluator[lit=1]"));
        }
        for (DataType type : DataType.stringTypes()) {
            params.add(new TestCase(new Literal(Source.EMPTY, new BytesRef(","), type), "LiteralsEvaluator[lit=,]"));
        }
        params.add(
            new TestCase(new Literal(Source.EMPTY, new Version("2.0").toBytesRef(), DataType.VERSION), "LiteralsEvaluator[lit=2.0]")
        );
        return params.stream().map(c -> new Object[] { c }).toList();
    }

    private final TestCase testCase;

    public LiteralsEvaluatorTests(TestCase testCase) {
        this.testCase = testCase;
    }

    public void testToString() {
        assertThat(factory().get(driverContext()).toString(), equalTo(testCase.expectedToString));
    }

    public void testFactoryToString() {
        assertThat(factory().toString(), equalTo(testCase.expectedToString));
    }

    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    private LiteralsEvaluator.Factory factory() {
        return new LiteralsEvaluator.Factory(testCase.lit);
    }
}
