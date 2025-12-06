/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.mapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.hamcrest.Matchers;

public class EvaluatorMapperTests extends ESTestCase {
    public void testFoldCompletesWithPlentyOfMemory() {
        Add add = new Add(
            Source.synthetic("shouldn't break"),
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            new Literal(Source.EMPTY, 3, DataType.INTEGER)
        );
        assertEquals(add.fold(new FoldContext(100)), 4);
    }

    public void testFoldBreaksWithLittleMemory() {
        Add add = new Add(
            Source.synthetic("should break"),
            new Literal(Source.EMPTY, 1, DataType.INTEGER),
            new Literal(Source.EMPTY, 3, DataType.INTEGER)
        );
        Exception e = expectThrows(FoldContext.FoldTooMuchMemoryException.class, () -> add.fold(new FoldContext(10)));
        assertThat(
            e.getMessage(),
            Matchers.equalTo(
                "line -1:-1: Folding query used more than 10b. "
                    + "The expression that pushed past the limit is [should break] which needed 32b."
            )
        );
    }
}
