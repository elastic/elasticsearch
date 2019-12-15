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
import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a conditional expression.
 */
public final class EConditional extends AExpression {

    private AExpression condition;
    private AExpression left;
    private AExpression right;

    public EConditional(Location location, AExpression condition, AExpression left, AExpression right) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    void extractVariables(Set<String> variables) {
        condition.extractVariables(variables);
        left.extractVariables(variables);
        right.extractVariables(variables);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        condition.expected = boolean.class;
        condition.analyze(scriptRoot, locals);
        condition = condition.cast(scriptRoot, locals);

        if (condition.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous conditional statement."));
        }

        left.expected = expected;
        left.explicit = explicit;
        left.internal = internal;
        right.expected = expected;
        right.explicit = explicit;
        right.internal = internal;
        actual = expected;

        left.analyze(scriptRoot, locals);
        right.analyze(scriptRoot, locals);

        if (expected == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        left = left.cast(scriptRoot, locals);
        right = right.cast(scriptRoot, locals);
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        Label fals = new Label();
        Label end = new Label();

        condition.write(classWriter, methodWriter, globals);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);

        left.write(classWriter, methodWriter, globals);
        methodWriter.goTo(end);
        methodWriter.mark(fals);
        right.write(classWriter, methodWriter, globals);
        methodWriter.mark(end);
    }

    @Override
    public String toString() {
        return singleLineToString(condition, left, right);
    }
}
