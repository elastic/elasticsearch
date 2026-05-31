/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class IrateTests extends AbstractIrateTests {
    public IrateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        super(testCaseSupplier);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();
        for (List<TestCaseSupplier.TypedDataSupplier> valuesSupplier : valuesSuppliers()) {
            for (TestCaseSupplier.TypedDataSupplier fieldSupplier : valuesSupplier) {
                for (RateTests.TemporalityParameter temporality : RateTests.TemporalityParameter.values()) {
                    suppliers.add(makeSupplier(fieldSupplier, temporality, true, "Irate"));
                }
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Irate(source, args.get(0), Literal.TRUE, AggregateFunction.NO_WINDOW, args.get(1), args.get(2));
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params, hasSize(5));
        assertThat(params.get(1).dataType(), equalTo(DataType.DATETIME));
        assertThat(params.get(2).dataType(), equalTo(DataType.KEYWORD));
        assertThat(params.get(3).dataType(), equalTo(DataType.INTEGER));
        assertThat(params.get(4).dataType(), equalTo(DataType.LONG));
        ArrayList<DocsV3Support.Param> result = new ArrayList<>();
        result.add(params.get(0));
        var preview = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", false);
        result.add(new DocsV3Support.Param(DataType.TIME_DURATION, List.of(preview)));
        return result;
    }
}
