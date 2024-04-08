/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ToLowerTests extends AbstractFunctionTestCase {
    public ToLowerTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        suppliers.add(supplier("keyword ascii", DataTypes.KEYWORD, () -> randomAlphaOfLengthBetween(1, 10)));
        suppliers.add(supplier("keyword unicode", DataTypes.KEYWORD, () -> randomUnicodeOfLengthBetween(1, 10)));
        suppliers.add(supplier("text ascii", DataTypes.TEXT, () -> randomAlphaOfLengthBetween(1, 10)));
        suppliers.add(supplier("text unicode", DataTypes.TEXT, () -> randomUnicodeOfLengthBetween(1, 10)));

        // add null as parameter
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(false, suppliers)));
    }

    public void testRandomLocale() {
        String testString = randomAlphaOfLength(10);
        EsqlConfiguration cfg = randomLocaleConfig();
        ToLower func = new ToLower(Source.EMPTY, new Literal(Source.EMPTY, testString, DataTypes.KEYWORD), cfg);
        assertThat(BytesRefs.toBytesRef(testString.toLowerCase(cfg.locale())), equalTo(func.fold()));
    }

    private EsqlConfiguration randomLocaleConfig() {
        return new EsqlConfiguration(
            DateUtils.UTC,
            randomLocale(random()),
            null,
            null,
            new QueryPragmas(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            "",
            false
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLower(source, args.get(0), EsqlTestUtils.TEST_CFG);
    }

    private static TestCaseSupplier supplier(String name, DataType type, Supplier<String> valueSupplier) {
        return new TestCaseSupplier(name, List.of(type), () -> {
            List<TestCaseSupplier.TypedData> values = new ArrayList<>();
            String expectedToString = "ToLowerEvaluator[val=Attribute[channel=0], locale=en_US]";

            String value = valueSupplier.get();
            values.add(new TestCaseSupplier.TypedData(new BytesRef(value), type, "0"));

            String expectedValue = value.toLowerCase(EsqlTestUtils.TEST_CFG.locale());
            return new TestCaseSupplier.TestCase(values, expectedToString, type, equalTo(new BytesRef(expectedValue)));
        });
    }
}
