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

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.hamcrest.Matchers.equalTo;

public class TopSnippetsErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(TopSnippetsTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        Set<List<DataType>> validWithAcceptedTypes = new HashSet<>(valid);
        for (DataType fieldDataType : DataType.stringTypes()) {
            if (DataType.UNDER_CONSTRUCTION.contains(fieldDataType)) {
                continue;
            }
            for (DataType queryDataType : DataType.stringTypes()) {
                if (DataType.UNDER_CONSTRUCTION.contains(queryDataType)) {
                    continue;
                }
                validWithAcceptedTypes.add(List.of(fieldDataType, queryDataType));
                validWithAcceptedTypes.add(List.of(fieldDataType, queryDataType, DataType.UNSUPPORTED));
            }
        }
        return super.testCandidates(cases, validWithAcceptedTypes).filter(sig -> false == sig.contains(DataType.NULL));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new TopSnippets(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(errorMessageStringForTopSnippets(validPerPosition, signature));
    }

    private static String errorMessageStringForTopSnippets(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        for (int i = 0; i < signature.size(); i++) {
            if (validDataTypeAtPosition(signature.get(i), i) == false) {
                // Map expressions have different error messages.
                if (i == 2) {
                    return format(null, "third argument of [{}] must be a map expression, received []", sourceForSignature(signature));
                }
                break;
            }
        }
        return typeErrorMessage(true, validPerPosition, signature, (v, p) -> "string");
    }

    private static boolean validDataTypeAtPosition(DataType dataType, int position) {
        return switch (position) {
            case 0, 1 -> DataType.isString(dataType);
            case 2 -> dataType == DataType.UNSUPPORTED;
            default -> false;
        };
    }
}
