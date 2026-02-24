/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.junit.BeforeClass;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Function tests for the abs2() function that extend AbstractScalarFunctionTestCase.
 * <p>
 * These tests enable documentation generation for the external abs2 function
 * using the same infrastructure as built-in functions.
 * </p>
 */
public class Abs2Tests extends AbstractScalarFunctionTestCase {

    @BeforeClass
    public static void registerFunction() {
        registerTestFunction(EsqlFunctionRegistry.createRuntimeDef(Abs2.class, Abs2::new, "abs2"));
    }

    public Abs2Tests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Integer test cases
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER), () -> {
            int arg = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.INTEGER, "arg")),
                "Abs2IntEvaluator[field0=Attribute[channel=0]]",
                DataType.INTEGER,
                equalTo(Math.abs(arg))
            );
        }));

        // Unsigned long test cases - abs returns the same value
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "Attribute[channel=0]",
            DataType.UNSIGNED_LONG,
            (n) -> n,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );

        // Long test cases
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG), () -> {
            long arg = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.LONG, "arg")),
                "Abs2LongEvaluator[field0=Attribute[channel=0]]",
                DataType.LONG,
                equalTo(Math.abs(arg))
            );
        }));

        // Double test cases
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE), () -> {
            double arg = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(arg, DataType.DOUBLE, "arg")),
                "Abs2DoubleEvaluator[field0=Attribute[channel=0]]",
                DataType.DOUBLE,
                equalTo(Math.abs(arg))
            );
        }));

        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Abs2(source, args.get(0));
    }

    /**
     * External functions are not registered in the core NamedWriteableRegistry,
     * so serialization tests are skipped.
     */
    @Override
    protected boolean canSerialize() {
        return false;
    }
}
