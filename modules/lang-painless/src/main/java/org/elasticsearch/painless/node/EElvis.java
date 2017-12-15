/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The Elvis operator ({@code ?:}), a null coalescing operator. Binary operator that evaluates the first expression and return it if it is
 * non null. If the first expression is null then it evaluates the second expression and returns it.
 */
public class EElvis extends AExpression {
    private AExpression lhs;
    private AExpression rhs;

    public EElvis(Location location, AExpression lhs, AExpression rhs) {
        super(location);

        this.lhs = requireNonNull(lhs);
        this.rhs = requireNonNull(rhs);
    }

    @Override
    void extractVariables(Set<String> variables) {
        lhs.extractVariables(variables);
        rhs.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        if (expected != null && expected.clazz.isPrimitive()) {
            throw createError(new IllegalArgumentException("Elvis operator cannot return primitives"));
        }
        lhs.expected = expected;
        lhs.explicit = explicit;
        lhs.internal = internal;
        rhs.expected = expected;
        rhs.explicit = explicit;
        rhs.internal = internal;
        actual = expected;
        lhs.analyze(locals);
        rhs.analyze(locals);

        if (lhs.isNull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is null."));
        }
        if (lhs.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a constant."));
        }
        if (lhs.actual.clazz.isPrimitive()) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a primitive."));
        }
        if (rhs.isNull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. RHS is null."));
        }

        if (expected == null) {
            final Type promote = locals.getDefinition().caster.promoteConditional(lhs.actual, rhs.actual, lhs.constant, rhs.constant);

            lhs.expected = promote;
            rhs.expected = promote;
            actual = promote;
        }

        lhs = lhs.cast(locals);
        rhs = rhs.cast(locals);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        Label end = new Label();

        lhs.write(writer, globals);
        writer.dup();
        writer.ifNonNull(end);
        writer.pop();
        rhs.write(writer, globals);
        writer.mark(end);
    }

    @Override
    public String toString() {
        return singleLineToString(lhs, rhs);
    }
}
