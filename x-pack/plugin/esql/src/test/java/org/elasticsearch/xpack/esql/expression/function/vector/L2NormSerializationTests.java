/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class L2NormSerializationTests extends AbstractVectorSimilaritySerializationTests<L2Norm> {
    @Override
    protected L2Norm create(Source source, Expression left, Expression right) {
        return new L2Norm(source, left, right);
    }
}
