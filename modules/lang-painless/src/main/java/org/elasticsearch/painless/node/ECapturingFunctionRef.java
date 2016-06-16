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

import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;

import java.lang.invoke.LambdaMetafactory;

/**
 * Represents a capturing function reference.
 */
public class ECapturingFunctionRef extends AExpression {
    public final String type;
    public final String call;
    
    private FunctionRef ref;
    Variable captured;
    String defPointer;

    public ECapturingFunctionRef(Location location, String type, String call) {
        super(location);

        this.type = type;
        this.call = call;
    }

    @Override
    void analyze(Locals variables) {
        captured = variables.getVariable(location, type);
        if (expected == null) {
            if (captured.type.sort == Definition.Sort.DEF) {
                // dynamic implementation
                defPointer = "D" + type + "." + call + ",1";
            } else {
                // typed implementation
                defPointer = "S" + captured.type.name + "." + call + ",1";
            }
            actual = Definition.getType("String");
        } else {
            defPointer = null;
            // static case
            if (captured.type.sort != Definition.Sort.DEF) {
                try {
                    ref = new FunctionRef(expected, captured.type.name, call, captured.type.clazz);
                } catch (IllegalArgumentException e) {
                    throw createError(e);
                }
            }
            actual = expected;
        }
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeDebugInfo(location);
        if (defPointer != null) {
            // dynamic interface: push captured parameter on stack
            // TODO: don't do this: its just to cutover :)
            writer.push((String)null);
            writer.visitVarInsn(captured.type.type.getOpcode(Opcodes.ILOAD), captured.slot);
        } else if (ref == null) {
            // typed interface, dynamic implementation
            writer.visitVarInsn(captured.type.type.getOpcode(Opcodes.ILOAD), captured.slot);
            String descriptor = Type.getMethodType(expected.type, captured.type.type).getDescriptor();
            writer.invokeDynamic(call, descriptor, DEF_BOOTSTRAP_HANDLE, DefBootstrap.REFERENCE, expected.name);
        } else {
            // typed interface, typed implementation
            writer.visitVarInsn(captured.type.type.getOpcode(Opcodes.ILOAD), captured.slot);
            // convert MethodTypes to asm Type for the constant pool.
            String invokedType = ref.invokedType.toMethodDescriptorString();
            Type samMethodType = Type.getMethodType(ref.samMethodType.toMethodDescriptorString());
            Type interfaceType = Type.getMethodType(ref.interfaceMethodType.toMethodDescriptorString());
            if (ref.needsBridges()) {
                writer.invokeDynamic(ref.invokedName, 
                                     invokedType, 
                                     LAMBDA_BOOTSTRAP_HANDLE, 
                                     samMethodType, 
                                     ref.implMethodASM, 
                                     samMethodType, 
                                     LambdaMetafactory.FLAG_BRIDGES, 
                                     1, 
                                     interfaceType);
            } else {
                writer.invokeDynamic(ref.invokedName, 
                                     invokedType, 
                                     LAMBDA_BOOTSTRAP_HANDLE, 
                                     samMethodType, 
                                     ref.implMethodASM, 
                                     samMethodType, 
                                     0);
            }
        }
    }
}
