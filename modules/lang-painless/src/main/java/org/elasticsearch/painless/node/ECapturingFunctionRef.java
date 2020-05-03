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

import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.Scope.Variable;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.TypedCaptureReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a capturing function reference.  For member functions that require a this reference, ie not static.
 */
public class ECapturingFunctionRef extends AExpression {

    protected final String variable;
    protected final String call;

    public ECapturingFunctionRef(Location location, String variable, String call) {
        super(location);

        this.variable = Objects.requireNonNull(variable);
        this.call = Objects.requireNonNull(call);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to capturing function reference [" + variable + ":"  + call + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: capturing function reference [" + variable + ":"  + call + "] not used"));
        }

        Output output = new Output();

        Variable captured = scope.getVariable(location, variable);
        if (input.expected == null) {
            String defReferenceEncoding;
            if (captured.getType() == def.class) {
                // unknown functional interface
                defReferenceEncoding = "D" + variable + "." + call + ",1";
            } else {
                // known functional interface
                defReferenceEncoding = "S" + captured.getCanonicalTypeName() + "." + call + ",1";
            }
            output.actual = String.class;

            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();

            defInterfaceReferenceNode.setLocation(location);
            defInterfaceReferenceNode.setExpressionType(output.actual);
            defInterfaceReferenceNode.addCapture(captured.getName());
            defInterfaceReferenceNode.setDefReferenceEncoding(defReferenceEncoding);

            output.expressionNode = defInterfaceReferenceNode;
        } else {
            output.actual = input.expected;
            // known functional interface
            if (captured.getType() != def.class) {
                FunctionRef ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(), location,
                        input.expected, captured.getCanonicalTypeName(), call, 1);

                TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
                typedInterfaceReferenceNode.setLocation(location);
                typedInterfaceReferenceNode.setExpressionType(output.actual);
                typedInterfaceReferenceNode.addCapture(captured.getName());
                typedInterfaceReferenceNode.setReference(ref);

                output.expressionNode = typedInterfaceReferenceNode;
            // known functional interface, unknown receiver type
            } else {
                TypedCaptureReferenceNode typedCaptureReferenceNode = new TypedCaptureReferenceNode();
                typedCaptureReferenceNode.setLocation(location);
                typedCaptureReferenceNode.setExpressionType(output.actual);
                typedCaptureReferenceNode.addCapture(captured.getName());
                typedCaptureReferenceNode.setMethodName(call);

                output.expressionNode = typedCaptureReferenceNode;
            }
        }

        return output;
    }
}
