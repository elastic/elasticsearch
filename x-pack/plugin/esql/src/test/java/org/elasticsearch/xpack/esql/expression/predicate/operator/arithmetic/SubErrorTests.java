/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.hamcrest.Matchers.equalTo;

public class SubErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(SubTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sub(source, args.get(0), args.get(1), ConfigurationTestUtils.randomConfiguration());
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.get(1) == DataType.DATETIME) {
            if (signature.getFirst().isNumeric()
                || DataType.isMillisOrNanos(signature.getFirst())
                || signature.getFirst() == DENSE_VECTOR) {
                return equalTo("[-] has arguments with incompatible types [" + signature.getFirst().typeName() + "] and [datetime]");
            }
            if (DataType.isNull(signature.getFirst())) {
                return equalTo("[-] arguments are in unsupported order: cannot subtract a [DATETIME] value [] from a [NULL] amount []");
            }
        }
        return equalTo(
            typeErrorMessage(true, validPerPosition, signature, (v, p) -> "date_nanos, datetime, numeric or dense_vector", () -> {
                if (signature.contains(DataType.UNSIGNED_LONG)
                    || signature.contains(DataType.DATETIME)
                    || signature.contains(DataType.DATE_NANOS)) {
                    return EsqlArithmeticOperation.formatIncompatibleTypesMessage("-", signature.get(0), signature.get(1));
                }
                throw new IllegalStateException("can't generate error for " + signature);
            })
        );
    }
}
