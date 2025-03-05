/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.ArrayList;
import java.util.List;

public final class ConvertStringToByteRef extends OptimizerRules.OptimizerExpressionRule<Literal> {

    public ConvertStringToByteRef() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(Literal lit, LogicalOptimizerContext ctx) {
        // TODO we shouldn't be emitting String into Literals at all
        Object value = lit.value();

        if (value == null) {
            return lit;
        }
        if (value instanceof String s) {
            return Literal.of(lit, new BytesRef(s));
        }
        if (value instanceof List<?> l) {
            if (l.isEmpty() || false == l.get(0) instanceof String) {
                return lit;
            }
            List<BytesRef> byteRefs = new ArrayList<>(l.size());
            for (Object v : l) {
                byteRefs.add(new BytesRef(v.toString()));
            }
            return Literal.of(lit, byteRefs);
        }
        return lit;
    }
}
