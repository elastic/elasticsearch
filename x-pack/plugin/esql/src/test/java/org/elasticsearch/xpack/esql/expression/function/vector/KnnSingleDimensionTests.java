/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * ES|QL folds a one-element multivalue into a scalar, so for a 1-dimension {@code dense_vector} the query vector
 * reaches {@link Knn} as a single {@link Float} literal rather than a list.
 */
public class KnnSingleDimensionTests extends ESTestCase {

    private static Knn knn(Object queryValue) {
        Source source = Source.EMPTY;
        return new Knn(
            source,
            Literal.keyword(source, "v"),
            new Literal(source, queryValue, DataType.DENSE_VECTOR),
            null,
            null,
            null,
            List.of()
        );
    }

    public void testSingleDimensionQueryVectorFoldedToScalar() {
        assertThat(knn(0.5f).queryAsObject(), equalTo(List.of(0.5f)));
    }

    public void testTwoDimensionQueryVector() {
        assertThat(knn(List.of(0.5f, 0.25f)).queryAsObject(), equalTo(List.of(0.5f, 0.25f)));
    }
}
