/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.stringCases;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("extract_snippets")
public class ExtractSnippetsTests extends AbstractFunctionTestCase {

    public ExtractSnippetsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(testCaseSuppliers());
    }

    private static List<TestCaseSupplier> testCaseSuppliers() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        addStringTestCases(suppliers);
        return suppliers;
    }

    public static void addStringTestCases(List<TestCaseSupplier> suppliers) {
        for (DataType fieldType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.containsKey(fieldType)) {
                continue;
            }
            for (TestCaseSupplier.TypedDataSupplier queryDataSupplier : stringCases(fieldType)) {
                suppliers.add(
                    TestCaseSupplier.testCaseSupplier(
                        queryDataSupplier,
                        new TestCaseSupplier.TypedDataSupplier(fieldType.typeName(), () -> randomAlphaOfLength(10), DataType.KEYWORD),
                        (d1, d2) -> equalTo("string"),
                        DataType.KEYWORD,
                        (o1, o2) -> true
                    )
                );
            }
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        ExtractSnippets extractSnippets = new ExtractSnippets(
            source,
            args.get(0),
            args.get(1),
            args.size() > 2 ? args.get(2) : null,
            args.size() > 3 ? args.get(3) : null
        );
        // We need to add the QueryBuilder to the extract_snippets expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense .
        if (args.get(0) instanceof FieldAttribute && args.get(1).foldable()) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, extractSnippets).toQueryBuilder();
            extractSnippets.replaceQueryBuilder(queryBuilder);
        }
        return extractSnippets;
    }
}
