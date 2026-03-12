/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.vector;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractUnaryScalarSerializationTests;
import org.junit.Before;

public class MagnitudeSerializationTests extends AbstractUnaryScalarSerializationTests<Magnitude> {

    @Before
    public void checkCapability() {
        assumeTrue("v_magnitude available in snapshot", EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled());
    }

    @Override
    protected Magnitude create(Source source, Expression child) {
        return new Magnitude(source, child);
    }
}
