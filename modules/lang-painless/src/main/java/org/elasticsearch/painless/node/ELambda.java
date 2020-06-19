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
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.LambdaScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.ReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
public class ELambda extends AExpression {

    private final List<String> canonicalTypeNameParameters;
    private final List<String> parameterNames;
    private final SBlock blockNode;

    public ELambda(int identifier, Location location,
            List<String> canonicalTypeNameParameters, List<String> parameterNames, SBlock blockNode) {

        super(identifier, location);

        this.canonicalTypeNameParameters = Collections.unmodifiableList(Objects.requireNonNull(canonicalTypeNameParameters));
        this.parameterNames = Collections.unmodifiableList(Objects.requireNonNull(parameterNames));
        this.blockNode = Objects.requireNonNull(blockNode);
    }

    public List<String> getCanonicalTypeNameParameters() {
        return canonicalTypeNameParameters;
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to a lambda"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: lambda not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();

        String name;
        Class<?> returnType;
        List<Class<?>> typeParametersWithCaptures;
        List<String> parameterNames;
        int maxLoopCounter;

        Output output = new Output();

        List<Class<?>> typeParameters;
        PainlessMethod interfaceMethod;
        // inspect the target first, set interface method if we know it.
        if (input.expected == null) {
            interfaceMethod = null;
            // we don't know anything: treat as def
            returnType = def.class;
            // don't infer any types, replace any null types with def
            typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());
            for (String type : canonicalTypeNameParameters) {
                if (type == null) {
                    typeParameters.add(def.class);
                } else {
                    Class<?> typeParameter = scriptScope.getPainlessLookup().canonicalTypeNameToType(type);

                    if (typeParameter == null) {
                        throw createError(new IllegalArgumentException("cannot resolve type [" + type + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }

        } else {
            // we know the method statically, infer return type and any unknown/def types
            interfaceMethod = scriptScope.getPainlessLookup().lookupFunctionalInterfacePainlessMethod(input.expected);
            if (interfaceMethod == null) {
                throw createError(new IllegalArgumentException("Cannot pass lambda to " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(input.expected) + "], not a functional interface"));
            }
            // check arity before we manipulate parameters
            if (interfaceMethod.typeParameters.size() != canonicalTypeNameParameters.size())
                throw new IllegalArgumentException("Incorrect number of parameters for [" + interfaceMethod.javaMethod.getName() +
                        "] in [" + PainlessLookupUtility.typeToCanonicalTypeName(input.expected) + "]");
            // for method invocation, its allowed to ignore the return value
            if (interfaceMethod.returnType == void.class) {
                returnType = def.class;
            } else {
                returnType = interfaceMethod.returnType;
            }
            // replace any null types with the actual type
            typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());
            for (int i = 0; i < canonicalTypeNameParameters.size(); i++) {
                String paramType = canonicalTypeNameParameters.get(i);
                if (paramType == null) {
                    typeParameters.add(interfaceMethod.typeParameters.get(i));
                } else {
                    Class<?> typeParameter = scriptScope.getPainlessLookup().canonicalTypeNameToType(paramType);

                    if (typeParameter == null) {
                        throw createError(new IllegalArgumentException("cannot resolve type [" + paramType + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }
        }

        LambdaScope lambdaScope = semanticScope.newLambdaScope(returnType);

        for (int index = 0; index < typeParameters.size(); ++index) {
            Class<?> type = typeParameters.get(index);
            String paramName = this.parameterNames.get(index);
            lambdaScope.defineVariable(getLocation(), type, paramName, true);
        }

        if (blockNode.getStatementNodes().isEmpty()) {
            throw createError(new IllegalArgumentException("cannot generate empty lambda"));
        }
        AStatement.Input blockInput = new AStatement.Input();
        blockInput.lastSource = true;
        AStatement.Output blockOutput = blockNode.analyze(classNode, lambdaScope, blockInput);

        if (blockOutput.methodEscape == false) {
            throw createError(new IllegalArgumentException("not all paths return a value for lambda"));
        }

        maxLoopCounter = scriptScope.getCompilerSettings().getMaxLoopCounter();

        // prepend capture list to lambda's arguments
        List<Variable> captures = new ArrayList<>(lambdaScope.getCaptures());
        typeParametersWithCaptures = new ArrayList<>(captures.size() + typeParameters.size());
        parameterNames = new ArrayList<>(captures.size() + this.parameterNames.size());
        for (Variable var : captures) {
            typeParametersWithCaptures.add(var.getType());
            parameterNames.add(var.getName());
        }
        typeParametersWithCaptures.addAll(typeParameters);
        parameterNames.addAll(this.parameterNames);

        // desugar lambda body into a synthetic method
        name = scriptScope.getNextSyntheticName("lambda");
        scriptScope.getFunctionTable().addFunction(name, returnType, typeParametersWithCaptures, true, true);

        ReferenceNode referenceNode;

        // setup method reference to synthetic method
        if (input.expected == null) {
            output.actual = String.class;
            String defReferenceEncoding = "Sthis." + name + "," + captures.size();

            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(defReferenceEncoding);
            referenceNode = defInterfaceReferenceNode;
        } else {
            FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                    getLocation(), input.expected, "this", name, captures.size());
            output.actual = input.expected;

            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(ref);
            referenceNode = typedInterfaceReferenceNode;
        }

        FunctionNode functionNode = new FunctionNode();
        functionNode.setBlockNode((BlockNode)blockOutput.statementNode);
        functionNode.setLocation(getLocation());
        functionNode.setName(name);
        functionNode.setReturnType(returnType);
        functionNode.getTypeParameters().addAll(typeParametersWithCaptures);
        functionNode.getParameterNames().addAll(parameterNames);
        functionNode.setStatic(true);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(true);
        functionNode.setMaxLoopCounter(maxLoopCounter);

        classNode.addFunctionNode(functionNode);

        referenceNode.setLocation(getLocation());
        referenceNode.setExpressionType(output.actual);

        for (Variable capture : captures) {
            referenceNode.addCapture(capture.getName());
        }

        output.expressionNode = referenceNode;

        return output;
    }
}
