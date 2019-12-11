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
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    private final List<String> paramTypeStrs;
    private final List<String> paramNameStrs;
    private final List<AStatement> statements;

    // extracted variables required to determine captures
    private final Set<String> extractedVariables;
    // desugared synthetic method (lambda body)
    private SFunction desugared;
    // captured variables
    private List<Variable> captures;
    // static parent, static lambda
    private FunctionRef ref;
    // dynamic parent, deferred until link time
    private String defPointer;

    public ELambda(Location location,
                   List<String> paramTypes, List<String> paramNames,
                   List<AStatement> statements) {
        super(location);
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);

        this.extractedVariables = new HashSet<>();
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AStatement statement : statements) {
            statement.extractVariables(extractedVariables);
        }

        variables.addAll(extractedVariables);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        Class<?> returnType;
        List<String> actualParamTypeStrs;
        PainlessMethod interfaceMethod;
        // inspect the target first, set interface method if we know it.
        if (expected == null) {
            interfaceMethod = null;
            // we don't know anything: treat as def
            returnType = def.class;
            // don't infer any types, replace any null types with def
            actualParamTypeStrs = new ArrayList<>(paramTypeStrs.size());
            for (String type : paramTypeStrs) {
                if (type == null) {
                    actualParamTypeStrs.add("def");
                } else {
                    actualParamTypeStrs.add(type);
                }
            }

        } else {
            // we know the method statically, infer return type and any unknown/def types
            interfaceMethod = scriptRoot.getPainlessLookup().lookupFunctionalInterfacePainlessMethod(expected);
            if (interfaceMethod == null) {
                throw createError(new IllegalArgumentException("Cannot pass lambda to " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(expected) + "], not a functional interface"));
            }
            // check arity before we manipulate parameters
            if (interfaceMethod.typeParameters.size() != paramTypeStrs.size())
                throw new IllegalArgumentException("Incorrect number of parameters for [" + interfaceMethod.javaMethod.getName() +
                        "] in [" + PainlessLookupUtility.typeToCanonicalTypeName(expected) + "]");
            // for method invocation, its allowed to ignore the return value
            if (interfaceMethod.returnType == void.class) {
                returnType = def.class;
            } else {
                returnType = interfaceMethod.returnType;
            }
            // replace any null types with the actual type
            actualParamTypeStrs = new ArrayList<>(paramTypeStrs.size());
            for (int i = 0; i < paramTypeStrs.size(); i++) {
                String paramType = paramTypeStrs.get(i);
                if (paramType == null) {
                    actualParamTypeStrs.add(PainlessLookupUtility.typeToCanonicalTypeName(interfaceMethod.typeParameters.get(i)));
                } else {
                    actualParamTypeStrs.add(paramType);
                }
            }
        }
        // any of those variables defined in our scope need to be captured
        captures = new ArrayList<>();
        for (String variable : extractedVariables) {
            if (locals.hasVariable(variable)) {
                captures.add(locals.getVariable(location, variable));
            }
        }
        // prepend capture list to lambda's arguments
        List<String> paramTypes = new ArrayList<>(captures.size() + actualParamTypeStrs.size());
        List<String> paramNames = new ArrayList<>(captures.size() + paramNameStrs.size());
        for (Variable var : captures) {
            paramTypes.add(PainlessLookupUtility.typeToCanonicalTypeName(var.clazz));
            paramNames.add(var.name);
        }
        paramTypes.addAll(actualParamTypeStrs);
        paramNames.addAll(paramNameStrs);

        // desugar lambda body into a synthetic method
        String name = scriptRoot.getNextSyntheticName("lambda");
        desugared = new SFunction(
                location, PainlessLookupUtility.typeToCanonicalTypeName(returnType), name, paramTypes, paramNames,
                new SBlock(location, statements), true);
        desugared.generateSignature(scriptRoot.getPainlessLookup());
        desugared.analyze(scriptRoot, Locals.newLambdaScope(locals.getProgramScope(), desugared.name, returnType,
                desugared.parameters, captures.size(), scriptRoot.getCompilerSettings().getMaxLoopCounter()));
        scriptRoot.getFunctionTable().addFunction(desugared.name, desugared.returnType, desugared.typeParameters, true);
        scriptRoot.getClassNode().addFunction(desugared);

        // setup method reference to synthetic method
        if (expected == null) {
            ref = null;
            actual = String.class;
            defPointer = "Sthis." + name + "," + captures.size();
        } else {
            defPointer = null;
            ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(),
                    location, expected, "this", desugared.name, captures.size());
            actual = expected;
        }
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (ref != null) {
            methodWriter.writeDebugInfo(location);
            // load captures
            for (Variable capture : captures) {
                methodWriter.visitVarInsn(MethodWriter.getType(capture.clazz).getOpcode(Opcodes.ILOAD), capture.getSlot());
            }

            methodWriter.invokeLambdaCall(ref);
        } else {
            // placeholder
            methodWriter.push((String)null);
            // load captures
            for (Variable capture : captures) {
                methodWriter.visitVarInsn(MethodWriter.getType(capture.clazz).getOpcode(Opcodes.ILOAD), capture.getSlot());
            }
        }
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public org.objectweb.asm.Type[] getCaptures() {
        org.objectweb.asm.Type[] types = new org.objectweb.asm.Type[captures.size()];
        for (int i = 0; i < types.length; i++) {
            types[i] = MethodWriter.getType(captures.get(i).clazz);
        }
        return types;
    }

    @Override
    public String toString() {
        return multilineToString(pairwiseToString(paramTypeStrs, paramNameStrs), statements);
    }
}
