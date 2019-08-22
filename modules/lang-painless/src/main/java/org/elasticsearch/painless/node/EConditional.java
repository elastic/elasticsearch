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
import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a conditional expression.
 */
public final class EConditional extends AExpression {

    public EConditional(Location location, AExpression condition, AExpression left, AExpression right) {
        super(location);

        children.add(Objects.requireNonNull(condition));
        children.add(Objects.requireNonNull(left));
        children.add(Objects.requireNonNull(right));
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        children.get(0).storeSettings(settings);
        children.get(1).storeSettings(settings);
        children.get(2).storeSettings(settings);
    }

    @Override
    void extractVariables(Set<String> variables) {
        children.get(0).extractVariables(variables);
        children.get(1).extractVariables(variables);
        children.get(2).extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        AExpression condition = (AExpression)children.get(0);
        AExpression left = (AExpression)children.get(1);
        AExpression right = (AExpression)children.get(2);

        condition.expected = boolean.class;
        condition.analyze(locals);
        children.set(0, condition = condition.cast(locals));

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

        left.analyze(locals);
        right.analyze(locals);

        if (expected == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        children.set(1, left = left.cast(locals));
        children.set(2, right = right.cast(locals));
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        Label fals = new Label();
        Label end = new Label();

        children.get(0).write(writer, globals);
        writer.ifZCmp(Opcodes.IFEQ, fals);

        children.get(1).write(writer, globals);
        writer.goTo(end);
        writer.mark(fals);
        children.get(2).write(writer, globals);
        writer.mark(end);
    }

    @Override
    public String toString() {
        return singleLineToString(children.get(0), children.get(1), children.get(2));
    }
}
