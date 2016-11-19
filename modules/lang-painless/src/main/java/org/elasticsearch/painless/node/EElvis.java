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
    /**
     * The label that null safe operations in the lhs can jump to if they encounter a null. Jumping there will pop the null off of the stack
     * and push the rhs on the stack.
     */
    private Label nullLabel;

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
        boolean lhsIsNullSafe = false;
        if (lhs instanceof IMaybeNullSafe) {
            IMaybeNullSafe maybeNullSafe = (IMaybeNullSafe) lhs;
            if (maybeNullSafe.isNullSafe()) {
                lhsIsNullSafe = true;
                maybeNullSafe.setDefaultForNull(this);
            }
        }
        lhsIsNullSafe = IMaybeNullSafe.applyIfPossible(lhs, this);
        if (false == lhsIsNullSafe && expected != null && expected.sort.primitive) {
            throw createError(new IllegalArgumentException("Evlis operator cannot return primitives"));
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
        if (lhs.actual.sort.primitive && false == lhsIsNullSafe) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a primitive."));
        }
        if (rhs.isNull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. RHS is null."));
        }

        if (expected == null) {
            final Type promote = AnalyzerCaster.promoteConditional(lhs.actual, rhs.actual, lhs.constant, rhs.constant);

            lhs.expected = promote;
            rhs.expected = promote;
            actual = promote;
        }

        lhs = lhs.cast(locals);
        rhs = rhs.cast(locals);
    }

    /**
     * Get the label that null safe operations in the lhs can jump to if they encounter a null. Jumping there will pop the null off the
     * stack and push the rhs.
     */
    Label nullLabel() {
        if (nullLabel == null) {
            nullLabel = new Label();
        }
        return nullLabel;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        Label end = new Label();
        lhs.write(writer, globals);
        if (lhs.actual.sort.primitive) {
            /* If the lhs primitive then it doesn't make sense to check it for null. The only way to get the rhs is if the lhs jumped there
             * on its own. We can only get here if the lhs is a null safe operation because we don't allow lhs to return a primitive
             * otherwise. */
            if (nullLabel == null) {
                throw createError(new IllegalStateException("Expected nullLabel to be created and consumed. This is a bug."));
            }
            writer.goTo(end);
        } else {
            writer.dup();
            writer.ifNonNull(end);
        }
        if (nullLabel != null) {
            // If the nullLabel was created by the writing the lhs then we need to mark it so it can be jumped to.
            writer.mark(nullLabel);
        }
        writer.pop();
        rhs.write(writer, globals);
        writer.mark(end);
    }
}
