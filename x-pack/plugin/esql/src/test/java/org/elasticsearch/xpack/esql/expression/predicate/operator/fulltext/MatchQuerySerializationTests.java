/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.fulltext;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MatchQuerySerializationTests extends AbstractFulltextSerializationTests<MatchQueryPredicate> {

    @Override
    protected final MatchQueryPredicate createTestInstance() {
        return new MatchQueryPredicate(randomSource(), randomChild(), randomAlphaOfLength(randomIntBetween(1, 16)), randomOptionOrNull());
    }

    @Override
    protected MatchQueryPredicate mutateInstance(MatchQueryPredicate instance) throws IOException {
        var field = instance.field();
        var query = instance.query();
        var options = instance.options();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> query = randomValueOtherThan(query, () -> randomAlphaOfLength(randomIntBetween(1, 16)));
            case 2 -> options = randomValueOtherThan(options, this::randomOptionOrNull);
        }
        return new MatchQueryPredicate(instance.source(), field, query, options);
    }
}
