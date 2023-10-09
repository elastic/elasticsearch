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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToStringTests extends AbstractFunctionTestCase {
    public ToStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ToStringFromIntEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            i -> new BytesRef(Integer.toString(i)),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToStringFromLongEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            l -> new BytesRef(Long.toString(l)),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToStringFromUnsignedLongEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            ul -> new BytesRef(ul.toString()),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToStringFromDoubleEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            d -> new BytesRef(Double.toString(d)),
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );
        TestCaseSupplier.forUnaryBoolean(
            suppliers,
            "ToStringFromBooleanEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            b -> new BytesRef(b.toString()),
            List.of()
        );
        TestCaseSupplier.forUnaryDatetime(
            suppliers,
            "ToStringFromDatetimeEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            i -> new BytesRef(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(i.toEpochMilli())),
            List.of()
        );
        TestCaseSupplier.forUnaryIp(
            suppliers,
            "ToStringFromIPEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            ip -> new BytesRef(DocValueFormat.IP.format(ip)),
            List.of()
        );
        TestCaseSupplier.forUnaryStrings(suppliers, read, DataTypes.KEYWORD, bytesRef -> bytesRef, List.of());
        TestCaseSupplier.forUnaryVersion(
            suppliers,
            "ToStringFromVersionEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            v -> new BytesRef(v.toString()),
            List.of()
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToString(source, args.get(0));
    }
}
