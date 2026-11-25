/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class JsonStringErrorTests extends ErrorsForCasesWithoutExamplesTestCase {
    @Override
    protected List<TestCaseSupplier> cases() {
        // Only keep the two-argument (single pair) signatures: error generation permutes over every
        // argument position and can't build the combinatorial explosion for longer signatures.
        return paramsToSuppliers(JsonStringTests.parameters()).stream().filter(c -> c.types().size() == 2).toList();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new JsonString(source, args);
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        // A null argument always resolves (a null key nulls the row, a null value becomes JSON null), so
        // signatures containing null aren't type errors and would need to be enumerated as valid instead.
        // We only check non-null signatures here.
        return super.testCandidates(cases, valid).filter(types -> types.stream().noneMatch(t -> t == DataType.NULL));
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        for (int i = 0; i < signature.size(); i++) {
            DataType type = signature.get(i);
            if (i % 2 == 0) {
                // key
                if (type != DataType.KEYWORD && type != DataType.TEXT) {
                    return typeErrorMessage(signature, i, "string");
                }
            } else {
                // value
                if (JsonString.ACCEPTED_VALUE_TYPES.contains(type) == false) {
                    return typeErrorMessage(signature, i, JsonString.VALUE_TYPES_MESSAGE);
                }
            }
        }
        throw new IllegalStateException("can't find bad arg for " + signature);
    }
}
