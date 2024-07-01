/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.util.List;

public abstract class AbstractMvSerializationTests<T extends Expression> extends AbstractExpressionSerializationTests<T> {
    @Override
    protected List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return AbstractMultivalueFunction.getNamedWriteables();
    }
}
