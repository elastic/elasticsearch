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
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToStringTests extends AbstractFunctionTestCase {
    public ToStringTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
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
            i -> new BytesRef(Integer.toString(i))
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToStringFromLongEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            l -> new BytesRef(Long.toString(l))
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToStringFromUnsignedLongEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            ul -> new BytesRef(ul.toString())
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToStringFromDoubleEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            d -> new BytesRef(Double.toString(d))
        );
        TestCaseSupplier.forUnaryBoolean(
            suppliers,
            "ToStringFromBooleanEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            b -> new BytesRef(b.toString())
        );
        TestCaseSupplier.forUnaryDatetime(
            suppliers,
            "ToStringFromDatetimeEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            i -> new BytesRef(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(i.toEpochMilli()))
        );
        TestCaseSupplier.forUnaryIp(
            suppliers,
            "ToStringFromIPEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            ip -> new BytesRef(DocValueFormat.IP.format(ip))
        );
        TestCaseSupplier.forUnaryStrings(suppliers, read, DataTypes.KEYWORD, bytesRef -> bytesRef);
        TestCaseSupplier.forUnaryVersion(
            suppliers,
            "ToStringFromVersionEvaluator[field=" + read + "]",
            DataTypes.KEYWORD,
            v -> new BytesRef(v.toString())
        );
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToString(source, args.get(0));
    }
}
