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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubDefNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a method call and defers to a child subnode.
 */
public class ECall extends AExpression {

    protected final AExpression prefix;
    protected final String name;
    protected final List<AExpression> arguments;
    protected final boolean nullSafe;

    public ECall(Location location, AExpression prefix, String name, List<AExpression> arguments, boolean nullSafe) {
        super(location);

        this.prefix = Objects.requireNonNull(prefix);
        this.name = Objects.requireNonNull(name);
        this.arguments = Collections.unmodifiableList(Objects.requireNonNull(arguments));
        this.nullSafe = nullSafe;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to method call [" + name + "/" + arguments.size() + "]"));
        }

        Output output = new Output();

        Input prefixInput = new Input();
        Output prefixOutput = prefix.analyze(classNode, scriptRoot, scope, prefixInput);

        if (prefixOutput.partialCanonicalTypeName != null) {
            throw createError(new IllegalArgumentException("cannot resolve symbol [" + prefixOutput.partialCanonicalTypeName + "]"));
        }

        ExpressionNode expressionNode;

        if (prefixOutput.actual == def.class) {
            if (output.isStaticType) {
                throw createError(new IllegalArgumentException("value required: " +
                        "instead found unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(output.actual) + "]"));
            }

            List<Output> argumentOutputs = new ArrayList<>(arguments.size());

            for (AExpression argument : arguments) {
                Input expressionInput = new Input();
                expressionInput.internal = true;
                Output expressionOutput = analyze(argument, classNode, scriptRoot, scope, expressionInput);
                argumentOutputs.add(expressionOutput);

                if (expressionOutput.actual == void.class) {
                    throw createError(new IllegalArgumentException(
                            "Argument(s) cannot be of [void] type when calling method [" + name + "]."));
                }
            }

            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            output.actual = input.expected == null || input.expected == ZonedDateTime.class || input.explicit ? def.class : input.expected;

            CallSubDefNode callSubDefNode = new CallSubDefNode();

            for (Output argumentOutput : argumentOutputs) {
                callSubDefNode.addArgumentNode(argumentOutput.expressionNode);
            }

            callSubDefNode.setLocation(location);
            callSubDefNode.setExpressionType(output.actual);
            callSubDefNode.setName(name);

            expressionNode = callSubDefNode;
        } else {
            PainlessMethod method = scriptRoot.getPainlessLookup().lookupPainlessMethod(
                    prefixOutput.actual, prefixOutput.isStaticType, name, arguments.size());

            if (method == null) {
                throw createError(new IllegalArgumentException(
                        "method [" + typeToCanonicalTypeName(prefixOutput.actual) + ", " + name + "/" + arguments.size() + "] not found"));
            }

            scriptRoot.markNonDeterministic(method.annotations.containsKey(NonDeterministicAnnotation.class));

            List<Output> argumentOutputs = new ArrayList<>(arguments.size());
            List<PainlessCast> argumentCasts = new ArrayList<>(arguments.size());

            for (int argument = 0; argument < arguments.size(); ++argument) {
                AExpression expression = arguments.get(argument);

                Input expressionInput = new Input();
                expressionInput.expected = method.typeParameters.get(argument);
                expressionInput.internal = true;
                Output expressionOutput = analyze(expression, classNode, scriptRoot, scope, expressionInput);
                argumentOutputs.add(expressionOutput);
                argumentCasts.add(AnalyzerCaster.getLegalCast(expression.location,
                        expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));

            }

            output.actual = method.returnType;

            CallSubNode callSubNode = new CallSubNode();

            for (int argument = 0; argument < arguments.size(); ++argument) {
                callSubNode.addArgumentNode(cast(argumentOutputs.get(argument).expressionNode, argumentCasts.get(argument)));
            }

            callSubNode.setLocation(location);
            callSubNode.setExpressionType(output.actual);
            callSubNode.setMethod(method);
            callSubNode.setBox(prefixOutput.actual);
            expressionNode = callSubNode;
        }

        if (nullSafe) {
            if (output.actual.isPrimitive()) {
                throw new IllegalArgumentException("Result of null safe operator must be nullable");
            }

            NullSafeSubNode nullSafeSubNode = new NullSafeSubNode();
            nullSafeSubNode.setChildNode(expressionNode);
            nullSafeSubNode.setLocation(location);
            nullSafeSubNode.setExpressionType(output.actual);
            expressionNode = nullSafeSubNode;
        }

        CallNode callNode = new CallNode();

        callNode.setLeftNode(prefixOutput.expressionNode);
        callNode.setRightNode(expressionNode);

        callNode.setLocation(location);
        callNode.setExpressionType(output.actual);

        output.expressionNode = callNode;

        return output;
    }
}
