/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RuntimeBm25FieldSerializationTests extends AbstractExpressionSerializationTests<RuntimeBm25Field> {

    @Override
    protected RuntimeBm25Field createTestInstance() {
        return new RuntimeBm25Field(randomSource(), randomChild(), randomTerms(), randomStatistics());
    }

    @Override
    protected RuntimeBm25Field mutateInstance(RuntimeBm25Field instance) throws IOException {
        Expression field = instance.field();
        Expression terms = instance.terms();
        List<Expression> statistics = instance.statistics();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> terms = randomValueOtherThan(terms, RuntimeBm25FieldSerializationTests::randomTerms);
            case 2 -> statistics = randomValueOtherThan(statistics, RuntimeBm25FieldSerializationTests::randomStatistics);
            default -> throw new AssertionError("unexpected mutation");
        }
        return new RuntimeBm25Field(instance.source(), field, terms, statistics);
    }

    /** Writing to a node that predates the feature fails with a 400 instead of shipping an element it cannot read. */
    public void testWriteToOlderVersionFails() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(RuntimeBm25RowStats.ESQL_RUNTIME_MATCH_BM25);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(createTestInstance(), older));
        assertThat(e.getMessage(), containsString("is not supported in peer node's version"));
    }

    private static Expression randomTerms() {
        List<BytesRef> terms = randomList(0, 5, () -> new BytesRef(randomAlphaOfLength(between(1, 6)))).stream()
            .distinct()
            .sorted()
            .toList();
        return RuntimeBm25RowStats.termsLiteral(Source.EMPTY, terms);
    }

    private static List<Expression> randomStatistics() {
        return randomList(2, 6, AbstractExpressionSerializationTests::randomChild);
    }
}
