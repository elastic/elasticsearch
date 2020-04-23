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
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodNameDecoration;
import org.elasticsearch.painless.symbol.Decorations.ParameterNames;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ReferenceDecoration;
import org.elasticsearch.painless.symbol.Decorations.ReturnType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.TypeParameters;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.LambdaScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

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
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitLambda(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, ELambda userLambdaNode, SemanticScope semanticScope) {

        if (semanticScope.getCondition(userLambdaNode, Write.class)) {
            throw userLambdaNode.createError(new IllegalArgumentException("invalid assignment: cannot assign a value to a lambda"));
        }

        if (semanticScope.getCondition(userLambdaNode, Read.class) == false) {
            throw userLambdaNode.createError(new IllegalArgumentException("not a statement: lambda not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        TargetType targetType = semanticScope.getDecoration(userLambdaNode, TargetType.class);
        List<String> canonicalTypeNameParameters = userLambdaNode.getCanonicalTypeNameParameters();

        Class<?> returnType;
        List<Class<?>> typeParameters;
        PainlessMethod interfaceMethod;

        // inspect the target first, set interface method if we know it.
        if (targetType == null) {
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
                        throw userLambdaNode.createError(new IllegalArgumentException("cannot resolve type [" + type + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }
        } else {
            // we know the method statically, infer return type and any unknown/def types
            interfaceMethod = scriptScope.getPainlessLookup().lookupFunctionalInterfacePainlessMethod(targetType.getTargetType());
            if (interfaceMethod == null) {
                throw userLambdaNode.createError(new IllegalArgumentException("Cannot pass lambda to " +
                        "[" + targetType.getTargetCanonicalTypeName() + "], not a functional interface"));
            }
            // check arity before we manipulate parameters
            if (interfaceMethod.typeParameters.size() != canonicalTypeNameParameters.size())
                throw new IllegalArgumentException("Incorrect number of parameters for [" + interfaceMethod.javaMethod.getName() +
                        "] in [" + targetType.getTargetCanonicalTypeName() + "]");
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
                        throw userLambdaNode.createError(new IllegalArgumentException("cannot resolve type [" + paramType + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }
        }

        Location location = userLambdaNode.getLocation();
        List<String> parameterNames = userLambdaNode.getParameterNames();
        LambdaScope lambdaScope = semanticScope.newLambdaScope(returnType);

        for (int index = 0; index < typeParameters.size(); ++index) {
            Class<?> type = typeParameters.get(index);
            String parameterName = parameterNames.get(index);
            lambdaScope.defineVariable(location, type, parameterName, true);
        }

        SBlock userBlockNode = userLambdaNode.getBlockNode();

        if (userBlockNode.getStatementNodes().isEmpty()) {
            throw userLambdaNode.createError(new IllegalArgumentException("cannot generate empty lambda"));
        }

        semanticScope.setCondition(userBlockNode, LastSource.class);
        visitor.visit(userBlockNode, lambdaScope);

        if (semanticScope.getCondition(userBlockNode, MethodEscape.class) == false) {
            throw userLambdaNode.createError(new IllegalArgumentException("not all paths return a value for lambda"));
        }

        // prepend capture list to lambda's arguments
        List<Variable> capturedVariables = new ArrayList<>(lambdaScope.getCaptures());
        List<Class<?>> typeParametersWithCaptures = new ArrayList<>(capturedVariables.size() + typeParameters.size());
        List<String> parameterNamesWithCaptures = new ArrayList<>(capturedVariables.size() + parameterNames.size());

        for (Variable capturedVariable : capturedVariables) {
            typeParametersWithCaptures.add(capturedVariable.getType());
            parameterNamesWithCaptures.add(capturedVariable.getName());
        }

        typeParametersWithCaptures.addAll(typeParameters);
        parameterNamesWithCaptures.addAll(parameterNames);

        // desugar lambda body into a synthetic method
        String name = scriptScope.getNextSyntheticName("lambda");
        scriptScope.getFunctionTable().addFunction(name, returnType, typeParametersWithCaptures, true, true);

        Class<?> valueType;
        // setup method reference to synthetic method
        if (targetType == null) {
            String defReferenceEncoding = "Sthis." + name + "," + capturedVariables.size();
            valueType = String.class;
            semanticScope.putDecoration(userLambdaNode, new EncodingDecoration(defReferenceEncoding));
        } else {
            FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                    location, targetType.getTargetType(), "this", name, capturedVariables.size());
            valueType = targetType.getTargetType();
            semanticScope.putDecoration(userLambdaNode, new ReferenceDecoration(ref));
        }

        semanticScope.putDecoration(userLambdaNode, new ValueType(valueType));
        semanticScope.putDecoration(userLambdaNode, new MethodNameDecoration(name));
        semanticScope.putDecoration(userLambdaNode, new ReturnType(returnType));
        semanticScope.putDecoration(userLambdaNode, new TypeParameters(typeParametersWithCaptures));
        semanticScope.putDecoration(userLambdaNode, new ParameterNames(parameterNamesWithCaptures));
        semanticScope.putDecoration(userLambdaNode, new CapturesDecoration(capturedVariables));
    }
}
