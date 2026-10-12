/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class StUnionErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(StUnionTests.parameters());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return args.size() == 1 ? new StUnionUnary(source, args.get(0)) : new StUnion(source, args.get(0), args.get(1));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (signature.size() == 1) {
            // Unary form: no ordinal in error message ("argument of [st_union]...")
            return equalTo(
                typeErrorMessage(false, validPerPosition, signature, (v, p) -> "geo_point, cartesian_point, geo_shape or cartesian_shape")
            );
        }
        // Binary form: ordinal included ("first/second argument of [st_union]...")
        return equalTo(SpatialContainsErrorTests.typeErrorMessage(true, validPerPosition, signature, false, false));
    }
}
