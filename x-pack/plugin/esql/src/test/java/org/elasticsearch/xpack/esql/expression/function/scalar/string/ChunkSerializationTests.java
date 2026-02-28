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
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChunkSerializationTests extends AbstractExpressionSerializationTests<Chunk> {
    @Override
    protected Chunk createTestInstance() {
        Source source = randomSource();
        Expression field = randomChild();
        Expression chunkingSettings = randomBoolean() ? null : randomChunkingSettings();
        return new Chunk(source, field, chunkingSettings);
    }

    @Override
    protected Chunk mutateInstance(Chunk instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression chunkingSettings = instance.chunkingSettings();
        switch (between(0, 1)) {
            case 0 -> field = randomValueOtherThan(field, AbstractUnaryScalarSerializationTests::randomChild);
            case 1 -> chunkingSettings = randomValueOtherThan(chunkingSettings, () -> randomBoolean() ? null : randomChunkingSettings());
        }
        return new Chunk(source, field, chunkingSettings);
    }

    private MapExpression randomChunkingSettings() {
        List<Expression> entries = new ArrayList<>();
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "strategy"));
            entries.add(Literal.keyword(Source.EMPTY, randomFrom("sentence", "word", "none")));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "max_chunk_size"));
            entries.add(new Literal(Source.EMPTY, randomIntBetween(20, 500), DataType.INTEGER));
        }
        if (randomBoolean()) {
            entries.add(Literal.keyword(Source.EMPTY, "sentence_overlap"));
            entries.add(new Literal(Source.EMPTY, randomFrom(0, 1), DataType.INTEGER));
        }
        return new MapExpression(Source.EMPTY, entries);
    }
}
