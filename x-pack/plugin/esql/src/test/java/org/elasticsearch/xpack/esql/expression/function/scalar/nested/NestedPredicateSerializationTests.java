/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class NestedPredicateSerializationTests extends AbstractExpressionSerializationTests<NestedPredicate> {
    @Override
    protected NestedPredicate createTestInstance() {
        int size = between(0, 3);
        return new NestedPredicate(
            randomSource(),
            randomPath(),
            randomChild(),
            randomInputs(size),
            randomRelativePaths(size),
            null,
            configuration()
        );
    }

    @Override
    protected NestedPredicate mutateInstance(NestedPredicate instance) throws IOException {
        String path = instance.path();
        Expression body = instance.body();
        List<Attribute> inputs = instance.inputs();
        List<String> relativePaths = instance.relativePaths();
        int size = inputs.size();
        switch (between(0, size == 0 ? 2 : 4)) {
            case 0 -> path = randomValueOtherThan(path, NestedPredicateSerializationTests::randomPath);
            case 1 -> body = randomValueOtherThan(body, AbstractExpressionSerializationTests::randomChild);
            case 2 -> {
                inputs = randomInputs(size + 1);
                relativePaths = randomRelativePaths(size + 1);
            }
            case 3 -> inputs = randomValueOtherThan(inputs, () -> randomInputs(size));
            case 4 -> relativePaths = randomValueOtherThan(relativePaths, () -> randomRelativePaths(size));
            default -> throw new UnsupportedOperationException();
        }
        return new NestedPredicate(instance.source(), path, body, inputs, relativePaths, null, instance.configuration());
    }

    public void testWriteToRefusesNodesWithoutNestedPredicates() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.randomVersionNotSupporting(NestedPredicate.VERSION));
            IOException e = expectThrows(IOException.class, () -> createTestInstance().writeTo(out));
            assertThat(e.getMessage(), containsString("NESTED_ANY requires nested predicate transport support"));
        }
    }

    public void testRoundTripsAnAnalyzedPredicate() throws IOException {
        SerializationTestUtils.assertSerialization(
            analyzedPredicate("FROM ne | WHERE NESTED_ANY(items.sku == \"a\" AND items.qty > 1) | LIMIT 10")
        );
    }

    public void testAQueryBuilderOfTheNodeThatPlansItIsNotSerialized() throws IOException {
        NestedPredicate predicate = analyzedPredicate("FROM ne | WHERE NESTED_ANY(items.sku == \"a\") | LIMIT 10").withQueryBuilder(
            QueryBuilders.matchAllQuery()
        );
        assertThat(predicate.queryBuilder(), is(notNullValue()));
        NestedPredicate copy = (NestedPredicate) SerializationTestUtils.serializeDeserialize(
            (Expression) predicate,
            PlanStreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(Expression.class)
        );
        assertThat(copy.queryBuilder(), is(nullValue()));
        assertThat(copy.path(), equalTo(predicate.path()));
        assertThat(copy.inputs().size(), equalTo(predicate.inputs().size()));
        assertThat(copy.relativePaths(), equalTo(predicate.relativePaths()));
    }

    public void testRoundTripsAPredicateOverTheCollection() throws IOException {
        SerializationTestUtils.assertSerialization(analyzedPredicate("FROM ne | EVAL hit = NESTED_ANY(items, true) | LIMIT 10"));
    }

    public static NestedPredicate analyzedPredicate(String query) {
        assumeTrue("NESTED_ANY is snapshot-only", Build.current().isSnapshot());
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
        List<NestedPredicate> found = new ArrayList<>();
        nestedAnalyzer(NestedPredicate.VERSION).query(query).forEachExpressionDown(NestedPredicate.class, found::add);
        return found.getFirst();
    }

    private static String randomPath() {
        return randomAlphaOfLength(5);
    }

    private static List<Attribute> randomInputs(int size) {
        List<Attribute> inputs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            inputs.add(randomReferenceAttribute(false));
        }
        return inputs;
    }

    private static List<String> randomRelativePaths(int size) {
        List<String> paths = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            paths.add(randomAlphaOfLength(4));
        }
        return paths;
    }
}
