/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction.expectedTypesAsString;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;
import static org.hamcrest.Matchers.equalTo;

public class MatchPhraseErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(MatchPhraseTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        Set<List<DataType>> validWithAcceptedTypes = new HashSet<>(valid);
        for (DataType fieldDataType : MatchPhrase.FIELD_DATA_TYPES) {
            for (DataType queryDataType : MatchPhrase.QUERY_DATA_TYPES) {
                validWithAcceptedTypes.add(List.of(fieldDataType, queryDataType));
                validWithAcceptedTypes.add(List.of(fieldDataType, queryDataType, DataType.UNSUPPORTED));
            }
        }
        return super.testCandidates(cases, validWithAcceptedTypes);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        MatchPhrase matchPhrase = new MatchPhrase(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
        // We need to add the QueryBuilder to the match_phrase expression, as it is used to implement equals() and hashCode() and
        // thus test the serialization methods. But we can only do this if the parameters make sense.
        if (args.get(0) instanceof FieldAttribute && args.get(1).foldable()) {
            QueryBuilder queryBuilder = TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, matchPhrase).toQueryBuilder();
            matchPhrase = (MatchPhrase) matchPhrase.replaceQueryBuilder(queryBuilder);
        }
        return matchPhrase;
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(
            errorMessageStringForMatchPhrase(
                validPerPosition,
                signature,
                (l, p) -> p == 0 ? expectedTypesAsString(MatchPhrase.FIELD_DATA_TYPES) : expectedTypesAsString(MatchPhrase.QUERY_DATA_TYPES)
            )
        );
    }

    private static String errorMessageStringForMatchPhrase(
        List<Set<DataType>> validPerPosition,
        List<DataType> signature,
        AbstractFunctionTestCase.PositionalErrorMessageSupplier positionalErrorMessageSupplier
    ) {
        for (int i = 0; i < signature.size(); i++) {
            // Need to check for nulls and bad parameters in order
            if (signature.get(i) == DataType.NULL && i > 0) {
                return TypeResolutions.ParamOrdinal.fromIndex(i).name().toLowerCase(Locale.ROOT)
                    + " argument of ["
                    + sourceForSignature(signature)
                    + "] cannot be null, received []";
            }
            if (validDataTypeAtPosition(signature.get(i), i) == false) {
                // Map expressions have different error messages.
                if (i == 2) {
                    return format(null, "third argument of [{}] must be a map expression, received []", sourceForSignature(signature));
                }
                break;
            }
        }

        return typeErrorMessage(true, validPerPosition, signature, positionalErrorMessageSupplier);
    }

    private static boolean validDataTypeAtPosition(DataType dataType, int position) {
        return switch (position) {
            case 0 -> MatchPhrase.FIELD_DATA_TYPES.contains(dataType);
            case 1 -> MatchPhrase.QUERY_DATA_TYPES.contains(dataType);
            case 2 -> dataType == DataType.UNSUPPORTED;
            default -> false;
        };
    }
}
