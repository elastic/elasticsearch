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
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;

/**
 * Represents a capturing function reference.
 */
public final class ECapturingFunctionRef extends AExpression implements ILambda {
    private final String variable;
    private final String call;

    private FunctionRef ref;
    private Variable captured;
    private String defPointer;

    public ECapturingFunctionRef(Location location, String variable, String call) {
        super(location);

        this.variable = Objects.requireNonNull(variable);
        this.call = Objects.requireNonNull(call);
    }

    @Override
    void extractVariables(Set<String> variables) {
        variables.add(variable);
    }

    @Override
    void analyze(Locals locals) {
        captured = locals.getVariable(location, variable);
        if (expected == null) {
            if (captured.type.dynamic) {
                // dynamic implementation
                defPointer = "D" + variable + "." + call + ",1";
            } else {
                // typed implementation
                defPointer = "S" + captured.type.name + "." + call + ",1";
            }
            actual = locals.getDefinition().getType("String");
        } else {
            defPointer = null;
            // static case
            if (captured.type.dynamic == false) {
                try {
                    ref = new FunctionRef(locals.getDefinition(), expected, captured.type.name, call, 1);

                    // check casts between the interface method and the delegate method are legal
                    for (int i = 0; i < ref.interfaceMethod.arguments.size(); ++i) {
                        Definition.Type from = ref.interfaceMethod.arguments.get(i);
                        Definition.Type to = ref.delegateMethod.arguments.get(i);
                        locals.getDefinition().caster.getLegalCast(location, from, to, false, true);
                    }

                    if (ref.interfaceMethod.rtn.equals(locals.getDefinition().voidType) == false) {
                        locals.getDefinition().caster.getLegalCast(location, ref.delegateMethod.rtn, ref.interfaceMethod.rtn, false, true);
                    }
                } catch (IllegalArgumentException e) {
                    throw createError(e);
                }
            }
            actual = expected;
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);
        if (defPointer != null) {
            // dynamic interface: push captured parameter on stack
            // TODO: don't do this: its just to cutover :)
            writer.push((String)null);
            writer.visitVarInsn(captured.type.type.getOpcode(Opcodes.ILOAD), captured.getSlot());
        } else if (ref == null) {
            // typed interface, dynamic implementation
            writer.visitVarInsn(captured.type.type.getOpcode(Opcodes.ILOAD), captured.getSlot());
            Type methodType = Type.getMethodType(expected.type, captured.type.type);
            writer.invokeDefCall(call, methodType, DefBootstrap.REFERENCE, expected.name);
        } else {
            // typed interface, typed implementation
            writer.visitVarInsn(captured.type.type.getOpcode(Opcodes.ILOAD), captured.getSlot());
            writer.invokeDynamic(
                ref.interfaceMethodName,
                ref.factoryDescriptor,
                LAMBDA_BOOTSTRAP_HANDLE,
                ref.interfaceType,
                ref.delegateClassName,
                ref.delegateInvokeType,
                ref.delegateMethodName,
                ref.delegateType
            );
        }
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public Type[] getCaptures() {
        return new Type[] { captured.type.type };
    }

    @Override
    public String toString() {
        return singleLineToString(variable, call);
    }
}
