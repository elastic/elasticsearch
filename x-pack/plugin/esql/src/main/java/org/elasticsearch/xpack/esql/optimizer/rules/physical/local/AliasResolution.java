/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

/**
 * Maps alias attributes to the expressions they stand for.
 */
record AliasResolution(AttributeMap<Expression> map) {
    static final AliasResolution EMPTY = new AliasResolution(AttributeMap.emptyAttributeMap());

    static AliasResolution of(EvalExec evalExec) {
        AttributeMap.Builder<Expression> builder = AttributeMap.builder();
        evalExec.fields().forEach(alias -> builder.put(alias.toAttribute(), alias.child()));
        return new AliasResolution(builder.build());
    }

    static AliasResolution of(ProjectExec projectExec) {
        AttributeMap.Builder<Expression> builder = AttributeMap.builder();
        for (NamedExpression ne : projectExec.projections()) {
            if (ne instanceof Alias alias) {
                builder.put(alias.toAttribute(), alias.child());
            }
        }
        return new AliasResolution(builder.build());
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Resolves {@code ref} to its full aliased expression (which may be a complex
     * expression), or returns {@code ref} unchanged if no mapping exists.
     */
    Expression resolveExpression(ReferenceAttribute ref) {
        return map.resolve(ref, ref);
    }

    /**
     * Resolves {@code ref} to the {@link Attribute} it was renamed from, or returns
     * {@code ref} unchanged if no mapping exists or if the mapped expression is not an
     * {@link Attribute}.
     */
    Attribute resolveRename(ReferenceAttribute ref) {
        Expression candidate = map.resolve(ref, ref);
        return candidate instanceof Attribute attr ? attr : ref;
    }
}
