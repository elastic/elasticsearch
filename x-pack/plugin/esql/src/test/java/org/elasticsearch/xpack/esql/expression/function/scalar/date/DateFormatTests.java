/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.matchesPattern;

public class DateFormatTests extends AbstractConfigurationFunctionTestCase {
    public DateFormatTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // Formatter supplied cases
        suppliers.addAll(
            TestCaseSupplier.forBinaryNotCasting(
                (format, value) -> new BytesRef(
                    DateFormatter.forPattern(((BytesRef) format).utf8ToString()).formatMillis(((Instant) value).toEpochMilli())
                ),
                DataType.KEYWORD,
                TestCaseSupplier.dateFormatCases(),
                TestCaseSupplier.dateCases(Instant.parse("1900-01-01T00:00:00.00Z"), Instant.parse("9999-12-31T00:00:00.00Z")),
                matchesPattern("DateFormatEvaluator\\[val=Attribute\\[channel=1], formatter=Attribute\\[(channel=0|\\w+)], locale=en_US]"),
                (lhs, rhs) -> List.of(),
                false
            )
        );
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new DateFormat(source, args.get(0), args.get(1), configuration);
    }
}
