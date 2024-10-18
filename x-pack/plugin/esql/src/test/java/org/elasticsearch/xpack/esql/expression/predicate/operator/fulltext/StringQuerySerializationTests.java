/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.fulltext;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.StringQueryPredicate;

import java.io.IOException;

public class StringQuerySerializationTests extends AbstractFulltextSerializationTests<StringQueryPredicate> {

    private static final String COMMA = ",";

    @Override
    protected final StringQueryPredicate createTestInstance() {
        return new StringQueryPredicate(randomSource(), randomAlphaOfLength(randomIntBetween(1, 16)), randomOptionOrNull());
    }

    @Override
    protected StringQueryPredicate mutateInstance(StringQueryPredicate instance) throws IOException {
        var query = instance.query();
        var options = instance.options();
        if (randomBoolean()) {
            query = randomValueOtherThan(query, () -> randomAlphaOfLength(randomIntBetween(1, 16)));
        } else {
            options = randomValueOtherThan(options, this::randomOptionOrNull);
        }
        return new StringQueryPredicate(instance.source(), query, options);
    }
}
