/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TopErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TopTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Top(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null, args.size() > 3 ? args.get(3) : null);
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // TODO TOP should handle nulls
        return super.testCandidates(cases, valid).filter(sig -> sig.size() < 4 || sig.get(3) != DataType.NULL);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        for (int i = 0; i < 3; i++) {
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
                    case 2 -> "keyword";
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
        if (signature.size() == 4) {
            if (signature.get(3) != DataType.NULL && validPerPosition.get(3).contains(signature.get(3)) == false) {
                return equalTo(
                    "fourth argument of ["
                        + sourceForSignature(signature)
                        + "] must be [date or numeric except unsigned_long or counter types], found value [] type ["
                        + signature.get(3).typeName()
                        + "]"
                );
            }
            DataType first = signature.getFirst();
            if (first != DataType.DATETIME && first.isNumeric() == false) {
                return equalTo(
                    "when fourth argument is set, first argument of ["
                        + sourceForSignature(signature)
                        + "] must be [date or numeric except unsigned_long or counter types], found value [] type ["
                        + first.typeName()
                        + "]"
                );
            }
        }
        throw new IllegalStateException("can't make error message for " + signature);
    }

    public void testLowLimit() {
        Top top = new Top(
            Source.synthetic(""),
            randomLiteral(DataType.LONG),
            new Literal(null, 0, DataType.INTEGER),
            new Literal(null, new BytesRef("desc"), DataType.KEYWORD),
            null
        );
        assertTrue(top.typeResolved().unresolved());
        assertThat(top.typeResolved().message(), equalTo("Limit must be greater than 0 in [], found [0]"));
    }

    public void testNonConstantLimit() {
        Expression limit = field("limit", DataType.INTEGER);
        Top top = new Top(
            Source.synthetic(""),
            randomLiteral(DataType.LONG),
            limit,
            new Literal(null, new BytesRef("desc"), DataType.KEYWORD),
            null
        );
        assertTrue(top.typeResolved().resolved());
        Failures failures = new Failures();
        top.postOptimizationVerification(failures);
        assertThat(failures.failures(), hasSize(1));
        assertThat(
            failures.failures().iterator().next().message(),
            equalTo("Limit must be a constant integer in [], found [" + limit + "]")
        );
    }

    public void testInvalidOrder() {
        Top top = new Top(
            Source.synthetic(""),
            randomLiteral(DataType.LONG),
            new Literal(null, 1, DataType.INTEGER),
            new Literal(null, new BytesRef("invalid"), DataType.KEYWORD),
            null
        );
        assertTrue(top.typeResolved().unresolved());
        assertThat(top.typeResolved().message(), equalTo("Invalid order value in [], expected [ASC, DESC] but got [invalid]"));
    }
}
