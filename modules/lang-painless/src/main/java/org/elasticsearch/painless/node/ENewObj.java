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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessConstructor;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents and object instantiation.
 */
public class ENewObj extends AExpression {

    private final String canonicalTypeName;
    private final List<AExpression> argumentNodes;

    public ENewObj(int identifier, Location location, String canonicalTypeName, List<AExpression> argumentNodes) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.argumentNodes = Collections.unmodifiableList(Objects.requireNonNull(argumentNodes));
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public List<AExpression> getArgumentNodes() {
        return argumentNodes;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitNewObj(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, ENewObj userNewObjNode, SemanticScope semanticScope) {

        String canonicalTypeName =  userNewObjNode.getCanonicalTypeName();
        List<AExpression> userArgumentNodes = userNewObjNode.getArgumentNodes();
        int userArgumentsSize = userArgumentNodes.size();

        if (semanticScope.getCondition(userNewObjNode, Write.class)) {
            throw userNewObjNode.createError(new IllegalArgumentException("invalid assignment cannot assign a value to new object with constructor " +
                    "[" + canonicalTypeName + "/" + userArgumentsSize + "]"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        Class<?> valueType = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw userNewObjNode.createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        PainlessConstructor constructor = scriptScope.getPainlessLookup().lookupPainlessConstructor(valueType, userArgumentsSize);

        if (constructor == null) {
            throw userNewObjNode.createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(valueType) + ", <init>/" + userArgumentsSize + "] not found"));
        }

        scriptScope.putDecoration(userNewObjNode, new StandardPainlessConstructor(constructor));
        scriptScope.markNonDeterministic(constructor.annotations.containsKey(NonDeterministicAnnotation.class));

        Class<?>[] types = new Class<?>[constructor.typeParameters.size()];
        constructor.typeParameters.toArray(types);

        if (constructor.typeParameters.size() != userArgumentsSize) {
            throw userNewObjNode.createError(new IllegalArgumentException(
                    "When calling constructor on type [" + PainlessLookupUtility.typeToCanonicalTypeName(valueType) + "] " +
                    "expected [" + constructor.typeParameters.size() + "] arguments, but found [" + userArgumentsSize + "]."));
        }

        for (int i = 0; i < userArgumentsSize; ++i) {
            AExpression userArgumentNode = userArgumentNodes.get(i);

            semanticScope.setCondition(userArgumentNode, Read.class);
            semanticScope.putDecoration(userArgumentNode, new TargetType(types[i]));
            semanticScope.setCondition(userArgumentNode, Internal.class);
            visitor.checkedVisit(userArgumentNode, semanticScope);
            visitor.decorateWithCast(userArgumentNode, semanticScope);
        }

        semanticScope.putDecoration(userNewObjNode, new ValueType(valueType));
    }
}
