/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * The two arms of {@code TYPED}'s evaluator, over a column of one type. Asking for the type the column already has
 * hands the child's block straight to the caller; asking for any other type discards it and answers null, and the
 * discarded block has to be closed or every page leaks - which the harness's own block accounting is what catches.
 * <p>
 * A column of one type is the only shape this harness can build, so what is covered here is the decision rather than
 * the selection it exists for. Selecting one leg of a genuine multi-type column needs a union block and is covered end
 * to end by {@code UnionTypedFieldIT}.
 * </p>
 */
public class TypedTests extends AbstractScalarFunctionTestCase {
    public TypedTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Every type the function's {@code field} parameter declares, because the harness checks the two lists against each
     * other and a type declared with no case here is a gap it refuses rather than ignores. The second argument names
     * the type in the mapping's vocabulary, which is {@link DataType#esType()} - {@code date} for {@code DATETIME},
     * the one type whose two names differ.
     */
    private static final List<DataType> TYPES = List.of(
        DataType.BOOLEAN,
        DataType.DATETIME,
        DataType.DATE_NANOS,
        DataType.DOUBLE,
        DataType.INTEGER,
        DataType.IP,
        DataType.KEYWORD,
        DataType.LONG,
        DataType.TEXT,
        DataType.UNSIGNED_LONG,
        DataType.VERSION
    );

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();
        for (DataType fieldType : TYPES) {
            // The matching arm: the value passes through untouched and the column keeps its type.
            cases.add(typedCase(fieldType, fieldType));
            // The discarding arm: a different type, so every row is null and the column takes the type asked for.
            for (DataType target : TYPES) {
                if (target != fieldType) {
                    cases.add(typedCase(fieldType, target));
                }
            }
        }
        // The plain overload, as every function in EXPRESSIONS_WITHOUT_ANY_NULL_IS_NULL uses: the default checks
        // generate a copy of each case with one argument nulled, and a null second argument is not a null value
        // here - it is a query naming no type, which resolveType refuses rather than answering null for.
        return parameterSuppliersFromTypedData(cases);
    }

    private static TestCaseSupplier typedCase(DataType fieldType, DataType target) {
        boolean matches = fieldType.widenSmallNumeric() == target.widenSmallNumeric();
        String name = "typed(" + fieldType.typeName() + ", \"" + target.esType() + "\")";
        return new TestCaseSupplier(name, List.of(fieldType, DataType.KEYWORD), () -> {
            Object value = EsqlTestUtils.randomLiteral(fieldType).value();
            // An unsigned_long rides in a long under an offset encoding and comes back out as the number it denotes,
            // so the expectation has to be decoded or it compares the two encodings of the same value.
            Object expected = fieldType == DataType.UNSIGNED_LONG ? NumericUtils.unsignedLongAsBigInteger((Long) value) : value;
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(value, fieldType, "field"),
                    new TestCaseSupplier.TypedData(BytesRefs.toBytesRef(target.esType()), DataType.KEYWORD, "type").forceLiteral()
                ),
                "TypedEvaluator[field=Attribute[channel=0],type=" + target + ",matches=" + matches + "]",
                // Narrowed the way dataType() narrows it: a text target selects a text leg and returns keyword.
                target.noText(),
                matches ? equalTo(expected) : nullValue()
            );
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Typed(source, args.get(0), args.get(1));
    }
}
