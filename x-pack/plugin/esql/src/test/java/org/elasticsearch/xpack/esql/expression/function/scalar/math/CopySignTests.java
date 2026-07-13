/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.casesCrossProduct;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.getCastEvaluator;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unlimitedSuppliers;
import static org.hamcrest.Matchers.equalTo;

public class CopySignTests extends AbstractScalarFunctionTestCase {
    public CopySignTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        List<DataType> numericTypes = List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE);

        for (DataType lhsType : numericTypes) {
            for (DataType rhsType : numericTypes) {
                BinaryOperator<Object> expected = (lhs, rhs) -> {
                    double sign = ((Number) rhs).doubleValue();
                    return switch (lhs) {
                        case Integer v -> {
                            if (sign < 0) {
                                yield v > 0 ? -v : v;
                            }
                            yield v > 0 ? v : -v;
                        }
                        case Long v -> {
                            if (sign < 0) {
                                yield v > 0 ? -v : v;
                            }
                            yield v > 0 ? v : -v;
                        }
                        case Double v -> Math.copySign(v, sign);
                        case Float v -> Math.copySign(v, sign);
                        default -> throw new IllegalArgumentException("unsupported [" + lhs.getClass() + "]");
                    };
                };
                BiFunction<DataType, DataType, Matcher<String>> evaluatorToString = (lhs, rhs) -> {
                    String name = "CopySign" + switch (lhs) {
                        case INTEGER -> "Integer";
                        case LONG -> "Long";
                        case DOUBLE -> "Double";
                        case FLOAT -> "Float";
                        default -> throw new IllegalStateException("unsupported [" + lhs + "]");
                    } + "Evaluator";
                    return equalTo(
                        name
                            + "[magnitude=Attribute[channel=0], sign="
                            + getCastEvaluator("Attribute[channel=1]", rhs, DataType.DOUBLE)
                            + "]"
                    );
                };
                casesCrossProduct(
                    expected,
                    unlimitedSuppliers(lhsType),
                    unlimitedSuppliers(rhsType),
                    evaluatorToString,
                    (l, r) -> List.of(),
                    suppliers,
                    lhsType,
                    false
                );
            }
        }

        return parameterSuppliersFromTypedData(
            anyNullIsNull(randomizeBytesRefsOffset(suppliers), (nullPosition, nullValueDataType, original) -> {
                if (nullPosition == 0 && nullValueDataType == DataType.NULL) {
                    return DataType.NULL;
                }
                return original.expectedType();
            }, (nullPosition, nullData, original) -> nullData.isForceLiteral() ? Matchers.equalTo("LiteralsEvaluator[lit=null]") : original)
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new CopySign(source, args.get(0), args.get(1));
    }
}
