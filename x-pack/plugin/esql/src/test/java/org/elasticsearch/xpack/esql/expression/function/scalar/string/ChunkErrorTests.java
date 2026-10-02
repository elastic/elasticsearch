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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class ChunkErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(ChunkTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        Set<List<DataType>> validWithAcceptedTypes = new HashSet<>(valid);
        validWithAcceptedTypes.add(List.of(DataType.NULL));
        validWithAcceptedTypes.add(List.of(DataType.NULL, DataType.UNSUPPORTED));
        for (DataType fieldDataType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.contains(fieldDataType)) {
                continue;
            }
            validWithAcceptedTypes.add(List.of(fieldDataType));
            validWithAcceptedTypes.add(List.of(fieldDataType, DataType.UNSUPPORTED));
        }
        return super.testCandidates(cases, validWithAcceptedTypes);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Chunk(source, args.get(0), args.size() > 1 ? args.get(1) : null);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(errorMessageStringForChunk(validPerPosition, signature));
    }

    private static String errorMessageStringForChunk(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        if (validDataTypeAtPosition(signature.get(0), 0) == false) {
            return typeErrorMessage(true, validPerPosition, signature, (v, p) -> "string");
        }
        return "invalid chunking_settings, found []";
    }

    private static boolean validDataTypeAtPosition(DataType dataType, int position) {
        return switch (position) {
            case 0 -> DataType.isString(dataType) || dataType == DataType.NULL;
            case 1 -> dataType == DataType.UNSUPPORTED;
            default -> false;
        };
    }
}
