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

import org.elasticsearch.painless.Definition.Type;
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

import java.lang.invoke.LambdaMetafactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;

/**
 * Lambda expression node.
 * <p>
 * This can currently only be the direct argument of a call (method/constructor).
 * When the argument is of a known type, it uses
 * <a href="http://cr.openjdk.java.net/~briangoetz/lambda/lambda-translation.html">
 * Java's lambda translation</a>. However, if its a def call, then we don't have
 * enough information, and have to defer this until link time. In that case a placeholder
 * and all captures are pushed onto the stack and folded into the signature of the parent call.
 * <p>
 * For example:
 * <br>
 * {@code def list = new ArrayList(); int capture = 0; list.sort((x,y) -> x - y + capture)}
 * <br>
 * is converted into a call (pseudocode) such as:
 * <br>
 * {@code sort(list, lambda$0, capture)}
 * <br>
 * At link time, when we know the interface type, this is decomposed with MethodHandle
 * combinators back into (pseudocode):
 * <br>
 * {@code sort(list, lambda$0(capture))}
 */
public final class ELambda extends AExpression implements ILambda {

    private final String name;
    private final FunctionReserved reserved;
    private final List<String> paramTypeStrs;
    private final List<String> paramNameStrs;
    private final List<AStatement> statements;

    // desugared synthetic method (lambda body)
    private SFunction desugared;
    // captured variables
    private List<Variable> captures;
    // static parent, static lambda
    private FunctionRef ref;
    // dynamic parent, deferred until link time
    private String defPointer;

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
        final Type returnType;
        final List<String> actualParamTypeStrs;
        Method interfaceMethod;
        // inspect the target first, set interface method if we know it.
        if (expected == null) {
            interfaceMethod = null;
            // we don't know anything: treat as def
            returnType = Definition.DEF_TYPE;
            // don't infer any types
            actualParamTypeStrs = paramTypeStrs;
        } else {
            // we know the method statically, infer return type and any unknown/def types
            interfaceMethod = expected.struct.getFunctionalMethod();
            if (interfaceMethod == null) {
                throw createError(new IllegalArgumentException("Cannot pass lambda to [" + expected.name +
                                                               "], not a functional interface"));
            }
            // check arity before we manipulate parameters
            if (interfaceMethod.arguments.size() != paramTypeStrs.size())
                throw new IllegalArgumentException("Incorrect number of parameters for [" + interfaceMethod.name +
                                                   "] in [" + expected.clazz + "]");
            // for method invocation, its allowed to ignore the return value
            if (interfaceMethod.rtn == Definition.VOID_TYPE) {
                returnType = Definition.DEF_TYPE;
            } else {
                returnType = interfaceMethod.rtn;
            }
            // replace any def types with the actual type (which could still be def)
            actualParamTypeStrs = new ArrayList<String>();
            for (int i = 0; i < paramTypeStrs.size(); i++) {
                String paramType = paramTypeStrs.get(i);
                if (paramType.equals(Definition.DEF_TYPE.name)) {
                    actualParamTypeStrs.add(interfaceMethod.arguments.get(i).name);
                } else {
                    actualParamTypeStrs.add(paramType);
                }
            }
        }
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
        paramTypes.addAll(actualParamTypeStrs);
        paramNames.addAll(paramNameStrs);

        // desugar lambda body into a synthetic method
        desugared = new SFunction(reserved, location, returnType.name, name,
                                            paramTypes, paramNames, statements, true);
        desugared.generateSignature();
        desugared.analyze(Locals.newLambdaScope(locals.getProgramScope(), returnType, desugared.parameters,
                                                captures.size(), reserved.getMaxLoopCounter()));

        // setup method reference to synthetic method
        if (expected == null) {
            ref = null;
            actual = Definition.getType("String");
            defPointer = "Sthis." + name + "," + captures.size();
        } else {
            defPointer = null;
            try {
                ref = new FunctionRef(expected, interfaceMethod, desugared.method, captures.size());
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
            org.objectweb.asm.Type samMethodType =
                org.objectweb.asm.Type.getMethodType(ref.samMethodType.toMethodDescriptorString());
            org.objectweb.asm.Type interfaceType =
                org.objectweb.asm.Type.getMethodType(ref.interfaceMethodType.toMethodDescriptorString());
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
    public org.objectweb.asm.Type[] getCaptures() {
        org.objectweb.asm.Type[] types = new org.objectweb.asm.Type[captures.size()];
        for (int i = 0; i < types.length; i++) {
            types[i] = captures.get(i).type.type;
        }
        return types;
    }
}
