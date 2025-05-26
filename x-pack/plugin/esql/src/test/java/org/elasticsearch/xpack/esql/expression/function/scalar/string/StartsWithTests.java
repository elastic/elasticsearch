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
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StartsWithTests extends AbstractScalarFunctionTestCase {
    public StartsWithTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType strType : DataType.stringTypes()) {
            for (DataType prefixType : DataType.stringTypes()) {
                suppliers.add(new TestCaseSupplier(List.of(strType, prefixType), () -> {
                    String str = randomAlphaOfLength(5);
                    String prefix = randomAlphaOfLength(5);
                    if (randomBoolean()) {
                        str = prefix + str;
                    }
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(str), strType, "str"),
                            new TestCaseSupplier.TypedData(new BytesRef(prefix), prefixType, "prefix")
                        ),
                        "StartsWithEvaluator[str=Attribute[channel=0], prefix=Attribute[channel=1]]",
                        DataType.BOOLEAN,
                        equalTo(str.startsWith(prefix))
                    );
                }));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StartsWith(source, args.get(0), args.get(1));
    }

    public void testLuceneQuery_AllLiterals_NonTranslatable() {
        var function = new StartsWith(
            Source.EMPTY,
            new Literal(Source.EMPTY, "test", DataType.KEYWORD),
            new Literal(Source.EMPTY, "test", DataType.KEYWORD)
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testLuceneQuery_NonFoldablePrefix_NonTranslatable() {
        var function = new StartsWith(
            Source.EMPTY,
            new FieldAttribute(Source.EMPTY, "field", new EsField("field", DataType.KEYWORD, Map.of(), true)),
            new FieldAttribute(Source.EMPTY, "field", new EsField("prefix", DataType.KEYWORD, Map.of(), true))
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testLuceneQuery_NonFoldablePrefix_Translatable() {
        var function = new StartsWith(
            Source.EMPTY,
            new FieldAttribute(Source.EMPTY, "field", new EsField("prefix", DataType.KEYWORD, Map.of(), true)),
            new Literal(Source.EMPTY, "a*b?c\\", DataType.KEYWORD)
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));

        var query = function.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new WildcardQuery(Source.EMPTY, "field", "a\\*b\\?c\\\\*")));
    }
}
