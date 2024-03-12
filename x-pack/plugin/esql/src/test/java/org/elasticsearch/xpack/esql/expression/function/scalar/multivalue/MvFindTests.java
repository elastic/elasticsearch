/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class MvFindTests extends AbstractScalarFunctionTestCase {
    public MvFindTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // pattern matches
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD, DataTypes.KEYWORD), () -> {
            List<String> field = randomList(1, 10, () -> randomAlphaOfLength(10));
            String randomItem = field.get(randomInt(field.size() - 1));
            BytesRef pattern = new BytesRef(randomItem.substring(0, randomInt(randomItem.length() - 1)));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(pattern, DataTypes.KEYWORD, "pattern")
                ),
                "MvFindEvaluator[field=Attribute[channel=0], pattern=" + pattern.utf8ToString() + "]",
                DataTypes.INTEGER,
                equalTo(expected(field, pattern.utf8ToString()))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.TEXT, DataTypes.TEXT), () -> {
            List<String> field = randomList(1, 10, () -> randomAlphaOfLength(10));
            String randomItem = field.get(randomInt(field.size() - 1));
            BytesRef pattern = new BytesRef(randomItem.substring(0, randomInt(randomItem.length() - 1)));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.TEXT, "field"),
                    new TestCaseSupplier.TypedData(pattern, DataTypes.TEXT, "pattern")
                ),
                "MvFindEvaluator[field=Attribute[channel=0], pattern=" + pattern.utf8ToString() + "]",
                DataTypes.INTEGER,
                equalTo(expected(field, pattern.utf8ToString()))
            );
        }));

        // pattern may not match
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD, DataTypes.KEYWORD), () -> {
            List<String> field = randomList(1, 10, () -> randomAlphaOfLength(10));
            BytesRef pattern = (BytesRef) randomLiteral(DataTypes.KEYWORD).value();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(pattern, DataTypes.KEYWORD, "pattern")
                ),
                "MvFindEvaluator[field=Attribute[channel=0], pattern=" + pattern.utf8ToString() + "]",
                DataTypes.INTEGER,
                equalTo(expected(field, pattern.utf8ToString()))
            );
        }));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static Integer expected(List<String> field, String pattern) {
        Integer match;
        Pattern p = Pattern.compile(pattern);
        for (int i = 0; i < field.size(); i++) {
            Matcher matcher = p.matcher(field.get(i));
            if (matcher.find()) {
                match = Integer.valueOf(i);
                return match;
            }
        }
        return null;
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.INTEGER;
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvFind(source, args.get(0), args.get(1));
    }

    @Override
    protected Expression buildFieldExpression(TestCaseSupplier.TestCase testCase) {
        List<Expression> args = new ArrayList<>(2);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        args.add(AbstractFunctionTestCase.field(data.get(0).name(), data.get(0).type()));
        args.add(new Literal(Source.synthetic(data.get(1).name()), data.get(1).data(), data.get(1).type()));
        return build(testCase.getSource(), args);
    }

    @Override
    protected Expression buildDeepCopyOfFieldExpression(TestCaseSupplier.TestCase testCase) {
        List<Expression> args = new ArrayList<>(2);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        args.add(AbstractFunctionTestCase.deepCopyOfField(data.get(0).name(), data.get(0).type()));
        args.add(new Literal(Source.synthetic(data.get(1).name()), data.get(1).data(), data.get(1).type()));
        return build(testCase.getSource(), args);
    }

    @Override
    public void testSimpleWithNulls() {
        assumeFalse("test case is invalid", false);
    }
}
