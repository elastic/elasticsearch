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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptRoot;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a boolean expression.
 */
public final class EBool extends AExpression {

    private final Operation operation;
    private AExpression left;
    private AExpression right;

    public EBool(Location location, Operation operation, AExpression left, AExpression right) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    void extractVariables(Set<String> variables) {
        left.extractVariables(variables);
        right.extractVariables(variables);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        left.expected = boolean.class;
        left.analyze(scriptRoot, locals);
        left = left.cast(scriptRoot, locals);

        right.expected = boolean.class;
        right.analyze(scriptRoot, locals);
        right = right.cast(scriptRoot, locals);

        if (left.constant != null && right.constant != null) {
            if (operation == Operation.AND) {
                constant = (boolean)left.constant && (boolean)right.constant;
            } else if (operation == Operation.OR) {
                constant = (boolean)left.constant || (boolean)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        if (operation == Operation.AND) {
            Label fals = new Label();
            Label end = new Label();

            left.write(classWriter, methodWriter, globals);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);
            right.write(classWriter, methodWriter, globals);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(true);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(false);
            methodWriter.mark(end);
        } else if (operation == Operation.OR) {
            Label tru = new Label();
            Label fals = new Label();
            Label end = new Label();

            left.write(classWriter, methodWriter, globals);
            methodWriter.ifZCmp(Opcodes.IFNE, tru);
            right.write(classWriter, methodWriter, globals);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.mark(tru);
            methodWriter.push(true);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(false);
            methodWriter.mark(end);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public String toString() {
        return singleLineToString(left, operation.symbol, right);
    }
}
