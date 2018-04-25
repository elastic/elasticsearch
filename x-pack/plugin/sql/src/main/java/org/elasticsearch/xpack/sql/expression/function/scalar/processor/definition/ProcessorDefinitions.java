/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;

public abstract class ProcessorDefinitions {

    public static ProcessorDefinition toProcessorDefinition(Expression ex) {
        if (ex.foldable()) {
            return new ConstantInput(ex.location(), ex, ex.fold());
        }
        if (ex instanceof ScalarFunction) {
            return ((ScalarFunction) ex).asProcessorDefinition();
        }
        if (ex instanceof AggregateFunction) {
            // unresolved AggNameInput (should always get replaced by the folder)
            return new AggNameInput(ex.location(), ex, ((AggregateFunction) ex).name());
        }
        if (ex instanceof NamedExpression) {
            return new AttributeInput(ex.location(), ex, ((NamedExpression) ex).toAttribute());
        }
        throw new SqlIllegalArgumentException("Cannot extract processor from {}", ex);
    }
}
