/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import java.util.Objects;

public class AggPathInput extends CommonNonExecutableInput<String> {

    private final String innerKey;
    // used in case the agg itself is not returned in a suitable format (like date aggs)
    private final Processor action;

    public AggPathInput(Expression expression, String context) {
        this(Location.EMPTY, expression, context, null, null);
    }

    public AggPathInput(Expression expression, String context, String innerKey) {
        this(Location.EMPTY, expression, context, innerKey, null);
    }

    public AggPathInput(Location location, Expression expression, String context, String innerKey, Processor action) {
        super(location, expression, context);
        this.innerKey = innerKey;
        this.action = action;
    }

    @Override
    protected NodeInfo<AggPathInput> info() {
        return NodeInfo.create(this, AggPathInput::new, expression(), context(), innerKey, action);
    }

    public String innerKey() {
        return innerKey;
    }

    public Processor action() {
        return action;
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return true;
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
                && Objects.equals(innerKey, other.innerKey)
                && Objects.equals(action, other.action);
    }
}
