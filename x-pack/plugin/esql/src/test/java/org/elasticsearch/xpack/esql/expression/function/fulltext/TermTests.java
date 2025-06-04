/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

@FunctionName("term")
public class TermTests extends AbstractFunctionTestCase {

    public TermTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Set<DataType>> supportedPerPosition = supportedParams();
        List<TestCaseSupplier> suppliers = new LinkedList<>();
        for (DataType fieldType : DataType.stringTypes()) {
            for (DataType queryType : DataType.stringTypes()) {
                addPositiveTestCase(List.of(fieldType, queryType), suppliers);
                addNonFieldTestCase(List.of(fieldType, queryType), supportedPerPosition, suppliers);
            }
        }

        return parameterSuppliersFromTypedData(suppliers);
    }

    protected static List<Set<DataType>> supportedParams() {
        Set<DataType> supportedTextParams = Set.of(DataType.KEYWORD, DataType.TEXT);
        Set<DataType> supportedNumericParams = Set.of(DataType.DOUBLE, DataType.INTEGER);
        Set<DataType> supportedFuzzinessParams = Set.of(DataType.INTEGER, DataType.KEYWORD, DataType.TEXT);
        List<Set<DataType>> supportedPerPosition = List.of(
            supportedTextParams,
            supportedTextParams,
            supportedNumericParams,
            supportedFuzzinessParams
        );
        return supportedPerPosition;
    }

    protected static void addPositiveTestCase(List<DataType> paramDataTypes, List<TestCaseSupplier> suppliers) {

        // Positive case - creates an ES field from the field parameter type
        suppliers.add(
            new TestCaseSupplier(
                getTestCaseName(paramDataTypes, "-ES field"),
                paramDataTypes,
                () -> new TestCaseSupplier.TestCase(
                    getTestParams(paramDataTypes),
                    "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                    DataType.BOOLEAN,
                    equalTo(true)
                )
            )
        );
    }

    private static void addNonFieldTestCase(
        List<DataType> paramDataTypes,
        List<Set<DataType>> supportedPerPosition,
        List<TestCaseSupplier> suppliers
    ) {
        // Negative case - use directly the field parameter type
        suppliers.add(
            new TestCaseSupplier(
                getTestCaseName(paramDataTypes, "-non ES field"),
                paramDataTypes,
                typeErrorSupplier(true, supportedPerPosition, paramDataTypes, TermTests::matchTypeErrorSupplier)
            )
        );
    }

    private static List<TestCaseSupplier.TypedData> getTestParams(List<DataType> paramDataTypes) {
        String fieldName = randomIdentifier();
        List<TestCaseSupplier.TypedData> params = new ArrayList<>();
        params.add(
            new TestCaseSupplier.TypedData(
                new FieldExpression(fieldName, List.of(new FieldExpression.FieldValue(fieldName))),
                paramDataTypes.get(0),
                "field"
            )
        );
        params.add(new TestCaseSupplier.TypedData(new BytesRef(randomAlphaOfLength(10)), paramDataTypes.get(1), "query"));
        return params;
    }

    private static String getTestCaseName(List<DataType> paramDataTypes, String fieldType) {
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        sb.append(paramDataTypes.get(0)).append(fieldType).append(", ");
        sb.append(paramDataTypes.get(1));
        sb.append(">");
        return sb.toString();
    }

    private static String matchTypeErrorSupplier(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        return "[] cannot operate on [" + types.getFirst().typeName() + "], which is not a field from an index mapping";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Match(source, List.of(args.get(0)), args.get(1), args.size() > 2 ? args.get(2) : null);
    }
}
