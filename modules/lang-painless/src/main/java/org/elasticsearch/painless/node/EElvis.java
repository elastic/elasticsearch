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
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class EElvis extends AExpression {
    private AExpression left;
    private AExpression right;
    private boolean unboxLhs;

    public EElvis(Location location, AExpression left, AExpression right) {
        super(location);

        this.left = requireNonNull(left);
        this.right = requireNonNull(right);
    }

    @Override
    void extractVariables(Set<String> variables) {
        left.extractVariables(variables);
        right.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        left.expected = expected;
        if (left.expected.sort.primitive) {
            left.expected = Definition.getType(left.expected.sort.boxed.getSimpleName());
            unboxLhs = true;
        }
        left.explicit = explicit;     // NOCOMMIT should this have the same de-primitive-ization
        left.internal = internal;
        right.expected = expected;
        right.explicit = explicit;
        right.internal = internal;
        actual = expected;
        left.analyze(locals);
        right.analyze(locals);

        if (left.isNull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is null."));
        }
        if (left.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a constant."));
        }
        if (left.actual.sort.primitive) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a primitive."));
        }
        if (right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. RHS is null."));
        }

        if (expected == null) {
            final Type promote = AnalyzerCaster.promoteConditional(left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        left = left.cast(locals);
        right = right.cast(locals);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        Label nul = new Label();
        Label end = new Label();

        left.write(writer, globals);
        writer.dup();
        if (unboxLhs) {
            writer.ifNull(nul);
            writer.unbox(actual.type);
            writer.goTo(end);
            writer.mark(nul);
        } else {
            writer.ifNonNull(end);
        }
        writer.pop();
        right.write(writer, globals);
        writer.mark(end);
    }
}
