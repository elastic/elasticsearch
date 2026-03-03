/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;

public class MatchPhraseSerializationTests extends AbstractExpressionSerializationTests<MatchPhrase> {
    @Override
    protected MatchPhrase createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        return new MatchPhrase(source, field, query, null);
    }

    @Override
    protected MatchPhrase mutateInstance(MatchPhrase instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression query = instance.query();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
        }
        return new MatchPhrase(source, field, query, null);
    }
}
