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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToVersionTests extends AbstractScalarFunctionTestCase {
    public ToVersionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        String stringEvaluator = "ToVersionFromStringEvaluator[field=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Converting and IP to an IP doesn't change anything. Everything should succeed.
        TestCaseSupplier.forUnaryVersion(suppliers, read, DataType.VERSION, Version::toBytesRef, List.of());

        // None of the random strings ever look like versions so they should all become "invalid" versions:
        // https://github.com/elastic/elasticsearch/issues/98989
        // TODO should this return null with warnings? they aren't version shaped at all.
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            stringEvaluator,
            DataType.VERSION,
            bytesRef -> new Version(bytesRef.utf8ToString()).toBytesRef(),
            List.of()
        );

        // But strings that are shaped like versions do parse to valid versions
        for (DataType inputType : DataType.stringTypes()) {
            TestCaseSupplier.unary(
                suppliers,
                read,
                TestCaseSupplier.versionCases(inputType.typeName() + " "),
                DataType.VERSION,
                bytesRef -> new Version((BytesRef) bytesRef).toBytesRef(),
                List.of()
            );
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> "string or version");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToVersion(source, args.get(0));
    }
}
