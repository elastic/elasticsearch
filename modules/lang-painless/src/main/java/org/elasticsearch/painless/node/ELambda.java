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
import org.elasticsearch.painless.Scope.LambdaScope;
import org.elasticsearch.painless.Scope.Variable;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.LambdaNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    // captured variables
    private List<Variable> captures;
    // static parent, static lambda
    private FunctionRef ref;
    // dynamic parent, deferred until link time
    private String defPointer;

    private String name;
    private Class<?> returnType;
    private List<Class<?>> typeParameters;
    private List<String> parameterNames;
    private SBlock block;
    private boolean methodEscape;
    private int maxLoopCounter;

    public ELambda(Location location,
                   List<String> paramTypes, List<String> paramNames,
                   List<AStatement> statements) {
        super(location);
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);

    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        List<Class<?>> typeParameters = new ArrayList<>();
        PainlessMethod interfaceMethod;
        // inspect the target first, set interface method if we know it.
        if (expected == null) {
            interfaceMethod = null;
            // we don't know anything: treat as def
            returnType = def.class;
            // don't infer any types, replace any null types with def
            typeParameters = new ArrayList<>(paramTypeStrs.size());
            for (String type : paramTypeStrs) {
                if (type == null) {
                    typeParameters.add(def.class);
                } else {
                    Class<?> typeParameter = scriptRoot.getPainlessLookup().canonicalTypeNameToType(type);

                    if (typeParameter == null) {
                        throw createError(new IllegalArgumentException("cannot resolve type [" + type + "]"));
                    }

                    typeParameters.add(typeParameter);
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
            typeParameters = new ArrayList<>(paramTypeStrs.size());
            for (int i = 0; i < paramTypeStrs.size(); i++) {
                String paramType = paramTypeStrs.get(i);
                if (paramType == null) {
                    typeParameters.add(interfaceMethod.typeParameters.get(i));
                } else {
                    Class<?> typeParameter = scriptRoot.getPainlessLookup().canonicalTypeNameToType(paramType);

                    if (typeParameter == null) {
                        throw createError(new IllegalArgumentException("cannot resolve type [" + paramType + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }
        }

        LambdaScope lambdaScope = scope.newLambdaScope(returnType);

        for (int index = 0; index < typeParameters.size(); ++index) {
            Class<?> type = typeParameters.get(index);
            String name = paramNameStrs.get(index);
            lambdaScope.defineVariable(location, type, name, true);
        }

        block = new SBlock(location, statements);
        if (block.statements.isEmpty()) {
            throw createError(new IllegalArgumentException("cannot generate empty lambda"));
        }
        block.lastSource = true;
        block.analyze(scriptRoot, lambdaScope);
        captures = new ArrayList<>(lambdaScope.getCaptures());

        methodEscape = block.methodEscape;

        if (methodEscape == false) {
            throw createError(new IllegalArgumentException("not all paths return a value for lambda"));
        }

        maxLoopCounter = scriptRoot.getCompilerSettings().getMaxLoopCounter();

        // prepend capture list to lambda's arguments
        this.typeParameters = new ArrayList<>(captures.size() + typeParameters.size());
        parameterNames = new ArrayList<>(captures.size() + paramNameStrs.size());
        for (Variable var : captures) {
            this.typeParameters.add(var.getType());
            parameterNames.add(var.getName());
        }
        this.typeParameters.addAll(typeParameters);
        parameterNames.addAll(paramNameStrs);

        // desugar lambda body into a synthetic method
        name = scriptRoot.getNextSyntheticName("lambda");
        scriptRoot.getFunctionTable().addFunction(name, returnType, this.typeParameters, true);

        // setup method reference to synthetic method
        if (expected == null) {
            ref = null;
            actual = String.class;
            defPointer = "Sthis." + name + "," + captures.size();
        } else {
            defPointer = null;
            ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(),
                    location, expected, "this", name, captures.size());
            actual = expected;
        }
    }

    @Override
    LambdaNode write(ClassNode classNode) {
        FunctionNode functionNode = new FunctionNode();

        functionNode.setBlockNode(block.write(classNode));

        functionNode.setLocation(location);
        functionNode.setName(name);
        functionNode.setReturnType(returnType);
        functionNode.getTypeParameters().addAll(typeParameters);
        functionNode.getParameterNames().addAll(parameterNames);
        functionNode.setSynthetic(true);
        functionNode.setMethodEscape(methodEscape);
        functionNode.setMaxLoopCounter(maxLoopCounter);

        classNode.addFunctionNode(functionNode);

        LambdaNode lambdaNode = new LambdaNode();

        lambdaNode.setLocation(location);
        lambdaNode.setExpressionType(actual);
        lambdaNode.setFuncRef(ref);

        for (Variable capture : captures) {
            lambdaNode.addCapture(capture.getName());
        }

        return lambdaNode;
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public List<Class<?>> getCaptures() {
        List<Class<?>> types = new ArrayList<>();
        for (Variable capture : captures) {
            types.add(capture.getType());
        }
        return types;
    }

    @Override
    public String toString() {
        return multilineToString(pairwiseToString(paramTypeStrs, paramNameStrs), statements);
    }
}
