/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;

import java.io.IOException;

public class TemporalityAttributeTests extends AbstractNamedExpressionSerializationTests<TemporalityAttribute> {

    @Override
    protected TemporalityAttribute mutateNameId(TemporalityAttribute instance) {
        return (TemporalityAttribute) instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }

    @Override
    protected TemporalityAttribute createTestInstance() {
        return new TemporalityAttribute(randomSource());
    }

    @Override
    protected TemporalityAttribute mutateInstance(TemporalityAttribute instance) throws IOException {
        return (TemporalityAttribute) instance.withId(new NameId());
    }
}
