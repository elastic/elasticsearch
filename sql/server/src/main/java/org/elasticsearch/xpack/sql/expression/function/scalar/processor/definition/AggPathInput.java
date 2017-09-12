/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;

import java.util.Objects;

public class AggPathInput extends UnresolvedInput<String> {

    private final String innerKey;

    public AggPathInput(Expression expression, String context) {
        this(expression, context, null);
    }

    public AggPathInput(Expression expression, String context, String innerKey) {
        super(expression, context);
        this.innerKey = innerKey;
    }

    public String innerKey() {
        return innerKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(context(), innerKey);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        AggPathInput other = (AggPathInput) obj;
        return Objects.equals(context(), other.context())
                && Objects.equals(innerKey, other.innerKey);
    }
}
