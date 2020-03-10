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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.UnaryNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a unary math expression.
 */
public final class EUnary extends AExpression {

    private final Operation operation;
    private AExpression child;

    private Class<?> promote;
    private boolean originallyExplicit = false; // record whether there was originally an explicit cast

    public EUnary(Location location, Operation operation, AExpression child) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.child = Objects.requireNonNull(child);
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        originallyExplicit = input.explicit;

        if (operation == Operation.NOT) {
            Input childInput = new Input();
            childInput.expected = boolean.class;
            child.analyze(scriptRoot, scope, childInput);
            child.cast();

            output.actual = boolean.class;
        } else if (operation == Operation.BWNOT || operation == Operation.ADD || operation == Operation.SUB) {
            Output childOutput = child.analyze(scriptRoot, scope, new Input());

            promote = AnalyzerCaster.promoteNumeric(childOutput.actual, operation != Operation.BWNOT);

            if (promote == null) {
                throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                        "[" + operation.symbol + "] to the type " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(childOutput.actual) + "]"));
            }

            child.input.expected = promote;
            child.cast();

            if (promote == def.class && input.expected != null) {
                output.actual = input.expected;
            } else {
                output.actual = promote;
            }
        } else {
            throw createError(new IllegalStateException("unexpected unary operation [" + operation.name + "]"));
        }

        return output;
    }

    @Override
    UnaryNode write(ClassNode classNode) {
        UnaryMathNode unaryMathNode = new UnaryMathNode();

        unaryMathNode.setChildNode(child.cast(child.write(classNode)));

        unaryMathNode.setLocation(location);
        unaryMathNode.setExpressionType(output.actual);
        unaryMathNode.setUnaryType(promote);
        unaryMathNode.setOperation(operation);
        unaryMathNode.setOriginallExplicit(originallyExplicit);

        return unaryMathNode;
    }

    @Override
    public String toString() {
        return singleLineToString(operation.symbol, child);
    }
}
