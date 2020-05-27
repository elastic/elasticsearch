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
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a unary math expression.
 */
public class EUnary extends AExpression {

    protected final Operation operation;
    protected final AExpression child;

    public EUnary(Location location, Operation operation, AExpression child) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.child = Objects.requireNonNull(child);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        Output output = new Output();

        Class<?> promote = null;
        boolean originallyExplicit = input.explicit;

        Input childInput = new Input();
        Output childOutput;

        ENumeric numeric = (ENumeric)child.getChildIf(ENumeric.class);
        EDecimal decimal = (EDecimal)child.getChildIf(EDecimal.class);

        if ((operation == Operation.SUB || operation == Operation.ADD) && (numeric != null || decimal != null)) {
            childInput.expected = input.expected;
            childInput.explicit = input.explicit;
            childInput.internal = input.internal;

            if (numeric != null) {
                if (operation == Operation.SUB) {
                    childOutput = numeric.analyze(classNode, scriptRoot, scope, childInput, numeric.value.charAt(0) != '-');
                } else {
                    childOutput = child.analyze(classNode, scriptRoot, scope, childInput);
                }
            } else if (decimal != null) {
                if (operation == Operation.SUB) {
                    childOutput = decimal.analyze(classNode, scriptRoot, scope, childInput, decimal.value.charAt(0) != '-');
                } else {
                    childOutput = child.analyze(classNode, scriptRoot, scope, childInput);
                }
            } else {
                throw createError(new IllegalArgumentException("illegal tree structure"));
            }

            output.actual = childOutput.actual;
            output.expressionNode = childOutput.expressionNode;
        } else {
            PainlessCast childCast;

            if (operation == Operation.NOT) {
                childInput.expected = boolean.class;
                childOutput = analyze(child, classNode, scriptRoot, scope, childInput);
                childCast = AnalyzerCaster.getLegalCast(child.location,
                        childOutput.actual, childInput.expected, childInput.explicit, childInput.internal);

                output.actual = boolean.class;
            } else if (operation == Operation.BWNOT || operation == Operation.ADD || operation == Operation.SUB) {
                childOutput = analyze(child, classNode, scriptRoot, scope, new Input());

                promote = AnalyzerCaster.promoteNumeric(childOutput.actual, operation != Operation.BWNOT);

                if (promote == null) {
                    throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                            "[" + operation.symbol + "] to the type " +
                            "[" + PainlessLookupUtility.typeToCanonicalTypeName(childOutput.actual) + "]"));
                }

                childInput.expected = promote;
                childCast = AnalyzerCaster.getLegalCast(child.location,
                        childOutput.actual, childInput.expected, childInput.explicit, childInput.internal);

                if (promote == def.class && input.expected != null) {
                    output.actual = input.expected;
                } else {
                    output.actual = promote;
                }
            } else {
                throw createError(new IllegalStateException("unexpected unary operation [" + operation.name + "]"));
            }

            UnaryMathNode unaryMathNode = new UnaryMathNode();
            unaryMathNode.setChildNode(cast(childOutput.expressionNode, childCast));
            unaryMathNode.setLocation(location);
            unaryMathNode.setExpressionType(output.actual);
            unaryMathNode.setUnaryType(promote);
            unaryMathNode.setOperation(operation);
            unaryMathNode.setOriginallExplicit(originallyExplicit);

            output.expressionNode = unaryMathNode;
        }

        return output;
    }
}
