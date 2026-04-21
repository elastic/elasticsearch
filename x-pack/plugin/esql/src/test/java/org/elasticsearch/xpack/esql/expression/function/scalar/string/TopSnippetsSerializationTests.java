/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TopSnippetsSerializationTests extends AbstractExpressionSerializationTests<TopSnippets> {
    @Override
    protected TopSnippets createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression query = randomChild();
        Expression options = randomBoolean() ? null : randomOptions();
        return new TopSnippets(source, field, query, options);
    }

    @Override
    protected TopSnippets mutateInstance(TopSnippets instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression query = instance.query();
        Expression options = instance.options();
        switch (between(0, 2)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> query = randomValueOtherThan(query, AbstractExpressionSerializationTests::randomChild);
            case 2 -> options = randomValueOtherThan(options, () -> randomBoolean() ? null : randomOptions());
        }
        return new TopSnippets(source, field, query, options);
    }

    private MapExpression randomOptions() {
        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "num_snippets"));
            entries.add(new Literal(Source.EMPTY, randomIntBetween(1, 20), DataType.INTEGER));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "num_words"));
            entries.add(new Literal(Source.EMPTY, randomIntBetween(0, 1000), DataType.INTEGER));
        }
        boolean highlight = randomBoolean();
        if (highlight) {
            entries.add(Literal.keyword(Source.EMPTY, "highlight"));
            entries.add(new Literal(Source.EMPTY, true, DataType.BOOLEAN));
            if (randomBoolean()) {
                entries.add(Literal.keyword(Source.EMPTY, "pre_tag"));
                entries.add(Literal.keyword(Source.EMPTY, randomFrom("<em>", "<b>", "<mark>")));
            }
            if (randomBoolean()) {
                entries.add(Literal.keyword(Source.EMPTY, "post_tag"));
                entries.add(Literal.keyword(Source.EMPTY, randomFrom("</em>", "</b>", "</mark>")));
            }
            if (randomBoolean()) {
                entries.add(Literal.keyword(Source.EMPTY, "encoder"));
                entries.add(Literal.keyword(Source.EMPTY, randomFrom("default", "html")));
            }
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "order"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("score", "none")));
        }
        return new MapExpression(Source.EMPTY, entries);
    }
}
