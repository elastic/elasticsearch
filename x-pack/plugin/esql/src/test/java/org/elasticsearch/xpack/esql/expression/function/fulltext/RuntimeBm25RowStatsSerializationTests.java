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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RuntimeBm25RowStatsSerializationTests extends AbstractExpressionSerializationTests<RuntimeBm25RowStats> {

    @Override
    protected RuntimeBm25RowStats createTestInstance() {
        return new RuntimeBm25RowStats(randomSource(), randomChild(), randomAnalyzer(), randomTerms());
    }

    @Override
    protected RuntimeBm25RowStats mutateInstance(RuntimeBm25RowStats instance) throws IOException {
        Expression field = instance.field();
        Expression analyzer = instance.analyzer();
        Expression terms = instance.terms();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> analyzer = randomValueOtherThan(analyzer, RuntimeBm25RowStatsSerializationTests::randomAnalyzer);
            case 2 -> terms = randomValueOtherThan(terms, RuntimeBm25RowStatsSerializationTests::randomTerms);
            default -> throw new AssertionError("unexpected mutation");
        }
        return new RuntimeBm25RowStats(instance.source(), field, analyzer, terms);
    }

    /** Writing to a node that predates the feature fails with a 400 instead of shipping a function it cannot read. */
    public void testWriteToOlderVersionFails() {
        TransportVersion older = TransportVersionUtils.getPreviousVersion(RuntimeBm25RowStats.ESQL_RUNTIME_MATCH_BM25);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(createTestInstance(), older));
        assertThat(e.getMessage(), containsString("is not supported in peer node's version"));
    }

    private static Expression randomAnalyzer() {
        return Literal.keyword(Source.EMPTY, randomFrom("standard", "whitespace", "simple", "stop"));
    }

    private static Expression randomTerms() {
        List<BytesRef> terms = randomList(0, 5, () -> new BytesRef(randomAlphaOfLength(between(1, 6)))).stream()
            .distinct()
            .sorted()
            .toList();
        return RuntimeBm25RowStats.termsLiteral(Source.EMPTY, terms);
    }
}
