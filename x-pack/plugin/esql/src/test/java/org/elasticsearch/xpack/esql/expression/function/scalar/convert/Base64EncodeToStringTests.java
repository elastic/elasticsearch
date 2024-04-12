/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

@FunctionName("base64_encode_tostring")
public class Base64EncodeToStringTests extends AbstractFunctionTestCase {
    public Base64EncodeToStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD), () -> {
            BytesRef input = (BytesRef) randomLiteral(DataTypes.KEYWORD).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(input, DataTypes.KEYWORD, "string")),
                "Base64EncodeToStringEvaluator[field=Attribute[channel=0]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(Base64.getEncoder().encode(input.utf8ToString().getBytes(StandardCharsets.UTF_8))))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.TEXT), () -> {
            BytesRef input = (BytesRef) randomLiteral(DataTypes.TEXT).value();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(input, DataTypes.TEXT, "string")),
                "Base64EncodeToStringEvaluator[field=Attribute[channel=0]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef(Base64.getEncoder().encode(input.utf8ToString().getBytes(StandardCharsets.UTF_8))))
            );
        }));

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Base64EncodeToString(source, args.get(0));
    }
}
