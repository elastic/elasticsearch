/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.union;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * A column whose indices agree on one type: every row answers with that type, which is the arm that carries every
 * query but the one the function exists for. The interesting arm - a column of more than one type - needs a union
 * block, which this harness cannot build, and is covered end to end by {@code UnionTypedFieldIT}.
 */
/*
 * The registered name is one word while the class is two, and the docs renderer derives the name it looks up
 * from the class - so without this the lookup misses, the docs and the Kibana definition are never generated,
 * and this class passes without having checked either.
 */
@FunctionName("typeof")
public class TypeOfTests extends AbstractScalarFunctionTestCase {
    public TypeOfTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        for (DataType type : List.of(
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.LONG,
            DataType.UNSIGNED_LONG,
            DataType.DOUBLE,
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.DATETIME,
            DataType.DATE_NANOS,
            DataType.IP,
            DataType.VERSION,
            DataType.FLATTENED
        )) {
            cases.add(
                new TestCaseSupplier(
                    "typeof(" + type.typeName() + ")",
                    List.of(type),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(EsqlTestUtils.randomLiteral(type).value(), type, "field")),
                        "TypeOfEvaluator[field=Attribute[channel=0],legs=null,type=" + type.esType() + "]",
                        DataType.KEYWORD,
                        // The mapping name, not the type's own name, so it matches the column's original_types.
                        equalTo(new BytesRef(type.esType()))
                    )
                )
            );
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TypeOf(source, args.get(0));
    }
}
