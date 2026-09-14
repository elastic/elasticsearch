/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

abstract class AbstractTopKErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    private final BiFunction<Source, List<Expression>, Expression> builder;

    AbstractTopKErrorTests(BiFunction<Source, List<Expression>, Expression> builder) {
        this.builder = builder;
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return builder.apply(source, args);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        for (int i = 0; i < 2; i++) {
            if (signature.get(i) == DataType.NULL) {
                return equalTo(
                    TypeResolutions.ParamOrdinal.fromIndex(i).toString().toLowerCase(Locale.ROOT)
                        + " argument of ["
                        + sourceForSignature(signature)
                        + "] cannot be null, received []"
                );
            }
            if (validPerPosition.get(i).contains(signature.get(i)) == false) {
                String expected = switch (i) {
                    case 0 -> "boolean, date, ip, string or numeric except unsigned_long or counter types";
                    case 1 -> "integer";
                    default -> "don't know";
                };
                return equalTo(
                    TypeResolutions.ParamOrdinal.fromIndex(i).toString().toLowerCase(Locale.ROOT)
                        + " argument of ["
                        + sourceForSignature(signature)
                        + "] must be ["
                        + expected
                        + "], found value [] type ["
                        + signature.get(i).typeName()
                        + "]"
                );
            }
        }
        throw new IllegalStateException("can't make error message for " + signature);
    }

    public void testLowLimit() {
        Expression topk = builder.apply(source(), List.of(randomLiteral(DataType.LONG), new Literal(null, 0, DataType.INTEGER)));
        assertTrue(topk.typeResolved().unresolved());
        assertThat(topk.typeResolved().message(), equalTo("Limit must be greater than 0 in [], found [0]"));
    }

    public void testNonConstantLimit() {
        Expression limit = field("limit", DataType.INTEGER);
        Expression topk = builder.apply(source(), List.of(randomLiteral(DataType.LONG), limit));
        assertTrue(topk.typeResolved().resolved());
        Failures failures = new Failures();
        ((PostOptimizationVerificationAware) topk).postOptimizationVerification(failures);
        assertThat(failures.failures(), hasSize(1));
        assertThat(
            failures.failures().iterator().next().message(),
            equalTo("Limit must be a constant integer in [], found [" + limit + "]")
        );
    }

    private Source source() {
        return Source.synthetic("");
    }
}
