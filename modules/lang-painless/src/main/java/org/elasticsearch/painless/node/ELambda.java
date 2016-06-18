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

import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.node.SFunction.FunctionReserved;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.LambdaMetafactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;

public class ELambda extends AExpression implements ILambda {
    final String name;
    final FunctionReserved reserved;
    final List<String> paramTypeStrs;
    final List<String> paramNameStrs;
    final List<AStatement> statements;

    // desugared synthetic method (lambda body)
    SFunction desugared;
    // captured variables
    List<Variable> captures;
    // static parent, static lambda
    FunctionRef ref;
    // dynamic parent, deferred until link time
    String defPointer;

    public ELambda(String name, FunctionReserved reserved, 
                   Location location, List<String> paramTypes, List<String> paramNames, 
                   List<AStatement> statements) {
        super(location);
        this.name = Objects.requireNonNull(name);
        this.reserved = Objects.requireNonNull(reserved);
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);
    }
    
    @Override
    void extractVariables(Set<String> variables) {
        for (AStatement statement : statements) {
            statement.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        // gather any variables used by the lambda body first.
        Set<String> variables = new HashSet<>();
        for (AStatement statement : statements) {
            statement.extractVariables(variables);
        }
        // any of those variables defined in our scope need to be captured
        captures = new ArrayList<>();
        for (String variable : variables) {
            if (locals.hasVariable(variable)) {
                captures.add(locals.getVariable(location, variable));
            }
        }
        // prepend capture list to lambda's arguments
        List<String> paramTypes = new ArrayList<>();
        List<String> paramNames = new ArrayList<>();
        for (Variable var : captures) {
            paramTypes.add(var.type.name);
            paramNames.add(var.name);
        }
        paramTypes.addAll(paramTypeStrs);
        paramNames.addAll(paramNameStrs);
        
        // desugar lambda body into a synthetic method
        desugared = new SFunction(reserved, location, "def", name, 
                                            paramTypes, paramNames, statements, true);
        desugared.generate();
        desugared.analyze(Locals.newLambdaScope(locals.getProgramScope(), desugared.parameters, 
                                                captures.size(), reserved.getMaxLoopCounter()));
        
        // setup method reference to synthetic method
        if (expected == null) {
            ref = null;
            actual = Definition.getType("String");
            defPointer = "Sthis." + name + "," + captures.size();
        } else {
            defPointer = null;
            try {
                Method interfaceMethod = expected.struct.getFunctionalMethod();
                if (interfaceMethod == null) {
                    throw new IllegalArgumentException("Cannot pass lambda to [" + expected.name + 
                            "], not a functional interface");
                }
                Class<?> captureClasses[] = new Class<?>[captures.size()];
                for (int i = 0; i < captures.size(); i++) {
                    captureClasses[i] = captures.get(i).type.clazz;
                }
                ref = new FunctionRef(expected, interfaceMethod, desugared.method, captureClasses);
            } catch (IllegalArgumentException e) {
                throw createError(e);
            }
            actual = expected;
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        if (ref != null) {
            writer.writeDebugInfo(location);
            // load captures
            for (Variable capture : captures) {
                writer.visitVarInsn(capture.type.type.getOpcode(Opcodes.ILOAD), capture.getSlot());
            }
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
        } else {
            // placeholder
            writer.push((String)null);
            // load captures
            for (Variable capture : captures) {
                writer.visitVarInsn(capture.type.type.getOpcode(Opcodes.ILOAD), capture.getSlot());
            }
        }

        // add synthetic method to the queue to be written
        globals.addSyntheticMethod(desugared);
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public Type[] getCaptures() {
        Type[] types = new Type[captures.size()];
        for (int i = 0; i < types.length; i++) {
            types[i] = captures.get(i).type.type;
        }
        return types;
    }
}
