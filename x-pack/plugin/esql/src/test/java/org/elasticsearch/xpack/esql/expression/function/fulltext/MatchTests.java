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

@FunctionName("match")
public class MatchTests extends AbstractFunctionTestCase {

    public MatchTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        Set<DataType> supportedTextParams = Set.of(DataType.KEYWORD, DataType.TEXT);
        Set<DataType> supportedNumericParams = Set.of(DataType.DOUBLE, DataType.INTEGER);
        Set<DataType> supportedFuzzinessParams = Set.of(DataType.INTEGER, DataType.KEYWORD, DataType.TEXT);
        List<Set<DataType>> supportedPerPosition = List.of(
            supportedTextParams,
            supportedTextParams,
            supportedNumericParams,
            supportedFuzzinessParams
        );
        List<TestCaseSupplier> suppliers = new LinkedList<>();
        for (DataType fieldType : DataType.stringTypes()) {
            for (DataType queryType : DataType.stringTypes()) {
                addPositiveTestCase(null, null, List.of(fieldType, queryType), supportedPerPosition, suppliers);
                addNonFieldTestCase(List.of(fieldType, queryType), supportedPerPosition, suppliers);
            }
        }

        // We can't add all combinations for all params - let's do it for the mandatory ones
        List<TestCaseSupplier> suppliersWithErrors = errorsForCasesWithoutExamples(suppliers, (v, p) -> "string");

        // Use valid values for the first two params, test boost
        for (Number boost : new Number[] { 2, 2.3 }) {
            List<DataType> paramDataTypes = List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.fromJava(boost));
            addPositiveTestCase(boost, null, paramDataTypes, supportedPerPosition, suppliersWithErrors);
        }

        validFunctionParameters().filter(dt -> dt.isNumeric() == false).forEach(dt -> {
            List<DataType> paramDataTypes = List.of(DataType.KEYWORD, DataType.KEYWORD, dt);
            suppliersWithErrors.add(
                new TestCaseSupplier(
                    "type error for boost " + TestCaseSupplier.nameFromTypes(paramDataTypes),
                    paramDataTypes,
                    typeErrorSupplier(true, supportedPerPosition, paramDataTypes, MatchTests::matchTypeErrorSupplier)
                )
            );
        });

        // Use valid values for the first three params, test boost
        for (Object fuzziness : new Object[] { 2, "AUTO" }) {
            List<DataType> paramDataTypes = List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.DOUBLE, DataType.fromJava(fuzziness));
            addPositiveTestCase(3.5, fuzziness, paramDataTypes, supportedPerPosition, suppliersWithErrors);
        }

        validFunctionParameters().filter(dt -> dt != DataType.INTEGER && dt != DataType.KEYWORD).forEach(dt -> {
            List<DataType> paramDataTypes = List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.DOUBLE, dt);
            suppliersWithErrors.add(
                new TestCaseSupplier(
                    "type error for fuzziness " + TestCaseSupplier.nameFromTypes(paramDataTypes),
                    paramDataTypes,
                    typeErrorSupplier(true, supportedPerPosition, paramDataTypes, MatchTests::matchTypeErrorSupplier)
                )
            );
        });

        // Don't test null, as it is not allowed but the expected message is not a type error - so we check it separately in VerifierTests
        return parameterSuppliersFromTypedData(
            suppliersWithErrors.stream().filter(s -> s.types().contains(DataType.NULL) == false).toList()
        );
    }

    private static void addPositiveTestCase(
        Number boost,
        Object fuzziness,
        List<DataType> paramDataTypes,
        List<Set<DataType>> supportedPerPosition,
        List<TestCaseSupplier> suppliers
    ) {

        // Positive case - creates an ES field from the field parameter type
        suppliers.add(
            new TestCaseSupplier(
                getTestCaseName(paramDataTypes, "-ES field", boost, fuzziness),
                paramDataTypes,
                () -> new TestCaseSupplier.TestCase(
                    getTestParams(paramDataTypes, boost, fuzziness),
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
                getTestCaseName(paramDataTypes, "-non ES field", null, null),
                paramDataTypes,
                typeErrorSupplier(true, supportedPerPosition, paramDataTypes, MatchTests::matchTypeErrorSupplier)
            )
        );
    }

    private static List<TestCaseSupplier.TypedData> getTestParams(List<DataType> paramDataTypes, Number boost, Object fuzziness) {
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
        if (paramDataTypes.size() > 2) {
            params.add(new TestCaseSupplier.TypedData(boost, paramDataTypes.get(2), "boost"));
        }
        if (paramDataTypes.size() > 3) {
            params.add(new TestCaseSupplier.TypedData(fuzziness, paramDataTypes.get(3), "fuzziness"));
        }
        return params;
    }

    private static String getTestCaseName(List<DataType> paramDataTypes, String fieldType, Number boost, Object fuzziness) {
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        sb.append(paramDataTypes.get(0)).append(fieldType).append(", ");
        sb.append(paramDataTypes.get(1));
        if (paramDataTypes.size() > 2) {
            sb.append(", ").append(paramDataTypes.get(2));
        }
        if (paramDataTypes.size() > 3) {
            sb.append(", ").append(paramDataTypes.get(3));
        }
        sb.append(">");
        return sb.toString();
    }

    private static String matchTypeErrorSupplier(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        return "[] cannot operate on [" + types.getFirst().typeName() + "], which is not a field from an index mapping";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        Expression field = args.get(0);
        Expression query = args.get(1);
        Expression boost = args.size() > 2 ? args.get(2) : null;
        Expression fuzziness = args.size() > 3 ? args.get(3) : null;

        return new Match(source, field, query, boost, fuzziness);
    }
}
