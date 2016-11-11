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

import java.util.Objects;
import java.util.Set;

public class EElvis extends AExpression {
    private AExpression left;
    private AExpression right;

    public EElvis(Location location, AExpression left, AExpression right) {
        super(location);

        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    void extractVariables(Set<String> variables) {
        left.extractVariables(variables);
        right.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        actual = expected;

        left.analyze(locals);
        right.analyze(locals);

        if (left.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a constant."));
        }
        if (left.actual.sort.primitive) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a primitive."));
        }

        if (expected == null) {
            final Type promote = AnalyzerCaster.promoteConditional(left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        Label nul = new Label();
        Label end = new Label();

        left.write(writer, globals);
        writer.dup();
        if (left.actual == actual) {
            writer.ifNonNull(end);
        } else {
            writer.ifNull(nul);
            // NOCOMMIT emit the cast
            writer.goTo(end);
            writer.mark(nul);
        }
        right.write(writer, globals);
        if (right.actual != actual) {
            // NOCOMMIT emit the cast
        }
        writer.mark(end);
    }
}
