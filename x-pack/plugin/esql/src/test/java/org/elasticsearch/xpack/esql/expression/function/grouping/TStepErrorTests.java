/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfigurationBuilder;
import static org.hamcrest.Matchers.equalTo;

public class TStepErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TStepTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        var configuration = randomConfigurationBuilder().query(source.text()).build();
        var anchor = configuration.now();
        return new TStep(source, args.get(0), args.get(1), configuration).withTimestampBounds(
            Literal.dateTime(source, anchor),
            Literal.dateTime(source, anchor)
        );
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        return super.testCandidates(cases, valid).filter(signature -> signature.contains(DataType.NULL) == false);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        int badArgPosition = -1;
        for (int i = 0; i < signature.size(); i++) {
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                badArgPosition = i;
                break;
            }
        }
        String source = sourceForSignature(signature);
        return switch (badArgPosition) {
            case 0 -> equalTo(
                "first argument of [" + source + "] must be [time_duration], found value [] type [" + signature.get(0).typeName() + "]"
            );
            case 1 -> equalTo(
                "implicit argument of ["
                    + source
                    + "] must be [date_nanos or datetime], found value [] type ["
                    + signature.get(1).typeName()
                    + "]"
            );
            default -> throw new IllegalStateException("Can't generate error message for signature " + signature);
        };
    }
}
