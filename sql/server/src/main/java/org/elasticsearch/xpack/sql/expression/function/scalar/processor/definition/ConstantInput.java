/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

public class ConstantInput extends LeafInput<Object> {

    public ConstantInput(Expression expression, Object context) {
        super(expression, context);
    }

    @Override
    public Processor asProcessor() {
        return new ConstantProcessor(context());
    }
}
