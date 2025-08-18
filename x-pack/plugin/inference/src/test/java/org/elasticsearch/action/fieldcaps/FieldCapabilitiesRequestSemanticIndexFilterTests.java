/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class FieldCapabilitiesRequestSemanticIndexFilterTests extends ESTestCase {
    private static final String EXPECTED_ERROR_MESSAGE = "index filter cannot contain semantic queries. Use an exists query instead.";

    public void testValidateWithoutIndexFilter() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.fields("field1", "field2");

        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);
    }

    public void testValidateWithNonSemanticIndexFilter() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.fields("field1", "field2");
        request.indexFilter(randomNonSemanticQuery());

        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);
    }

    public void testValidateWithDirectSemanticQuery() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
        request.fields("field1", "field2");
        request.indexFilter(randomSemanticQuery());

        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString(EXPECTED_ERROR_MESSAGE));
    }

    public void testValidateWithRandomCompoundQueryContainingSemantic() {
        for (int i = 0; i < 100; i++) {
            FieldCapabilitiesRequest request = new FieldCapabilitiesRequest();
            request.fields("field1", "field2");

            // Create a randomly structured compound query containing semantic query
            QueryBuilder randomCompoundQuery = randomCompoundQueryWithSemantic(randomIntBetween(1, 3));
            request.indexFilter(randomCompoundQuery);

            ActionRequestValidationException validationException = request.validate();
            assertThat(validationException, notNullValue());
            assertThat(validationException.getMessage(), containsString(EXPECTED_ERROR_MESSAGE));
        }
    }

    private static SemanticQueryBuilder randomSemanticQuery() {
        return new SemanticQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(10));
    }

    private static QueryBuilder randomNonSemanticQuery() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5));
            case 1 -> new MatchAllQueryBuilder();
            case 2 -> {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                boolQuery.must(new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLength(5)));
                yield boolQuery;
            }
            default -> throw new IllegalStateException("Unexpected value");
        };
    }

    private static QueryBuilder randomCompoundQueryWithSemantic(int depth) {
        if (depth <= 0) {
            return randomSemanticQuery();
        }

        return switch (randomIntBetween(0, 5)) {
            case 0 -> {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                QueryBuilder clauseQuery = randomCompoundQueryWithSemantic(depth - 1);
                switch (randomIntBetween(0, 3)) {
                    case 0 -> boolQuery.must(clauseQuery);
                    case 1 -> boolQuery.mustNot(clauseQuery);
                    case 2 -> boolQuery.should(clauseQuery);
                    case 3 -> boolQuery.filter(clauseQuery);
                    default -> throw new IllegalStateException("Unexpected value");
                }

                if (randomBoolean()) {
                    boolQuery.should(randomNonSemanticQuery());
                }

                yield boolQuery;
            }
            case 1 -> {
                DisMaxQueryBuilder disMax = new DisMaxQueryBuilder();
                disMax.add(randomCompoundQueryWithSemantic(depth - 1));
                if (randomBoolean()) {
                    disMax.add(randomNonSemanticQuery());
                }
                yield disMax;
            }
            case 2 -> new NestedQueryBuilder(randomAlphaOfLength(5), randomCompoundQueryWithSemantic(depth - 1), ScoreMode.Max);
            case 3 -> {
                boolean positiveSemanticQuery = randomBoolean();
                QueryBuilder semanticQuery = randomCompoundQueryWithSemantic(depth - 1);
                QueryBuilder nonSemanticQuery = randomNonSemanticQuery();

                yield new BoostingQueryBuilder(
                    positiveSemanticQuery ? semanticQuery : nonSemanticQuery,
                    positiveSemanticQuery ? nonSemanticQuery : semanticQuery
                );
            }
            case 4 -> new ConstantScoreQueryBuilder(randomCompoundQueryWithSemantic(depth - 1));
            case 5 -> new FunctionScoreQueryBuilder(randomCompoundQueryWithSemantic(depth - 1));
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
