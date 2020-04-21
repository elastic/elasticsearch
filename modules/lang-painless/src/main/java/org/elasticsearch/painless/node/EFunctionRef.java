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
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ReferenceDecoration;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Collections;
import java.util.Objects;

/**
 * Represents a function reference.
 */
public class EFunctionRef extends AExpression {

    private final String symbol;
    private final String methodName;

    public EFunctionRef(int identifier, Location location, String symbol, String methodName) {
        super(identifier, location);

        this.symbol = Objects.requireNonNull(symbol);
        this.methodName = Objects.requireNonNull(methodName);
    }

    public String getSymbol() {
        return symbol;
    }

    public String getCall() {
        return methodName;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitFunctionRef(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        ScriptScope scriptScope = semanticScope.getScriptScope();
        boolean read = semanticScope.getCondition(this, Read.class);
        TargetType targetType = semanticScope.getDecoration(this, TargetType.class);

        Class<?> valueType;
        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(symbol);

        if (symbol.equals("this") || type != null)  {
            if (semanticScope.getCondition(this, Write.class)) {
                throw createError(new IllegalArgumentException(
                        "invalid assignment: cannot assign a value to function reference [" + symbol + ":" + methodName + "]"));
            }

            if (read == false) {
                throw createError(new IllegalArgumentException(
                        "not a statement: function reference [" + symbol + ":" + methodName + "] not used"));
            }

            if (targetType == null) {
                valueType = String.class;
                String defReferenceEncoding = "S" + symbol + "." + methodName + ",0";
                semanticScope.putDecoration(this, new EncodingDecoration(defReferenceEncoding));
            } else {
                FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                        getLocation(), targetType.getTargetType(), symbol, methodName, 0);
                valueType = targetType.getTargetType();
                semanticScope.putDecoration(this, new ReferenceDecoration(ref));
            }
        } else {
            if (semanticScope.getCondition(this, Write.class)) {
                throw createError(new IllegalArgumentException(
                        "invalid assignment: cannot assign a value to capturing function reference [" + symbol + ":"  + methodName + "]"));
            }

            if (read == false) {
                throw createError(new IllegalArgumentException(
                        "not a statement: capturing function reference [" + symbol + ":"  + methodName + "] not used"));
            }

            SemanticScope.Variable captured = semanticScope.getVariable(getLocation(), symbol);
            semanticScope.putDecoration(this, new CapturesDecoration(Collections.singletonList(captured)));
            if (targetType == null) {
                String defReferenceEncoding;
                if (captured.getType() == def.class) {
                    // dynamic implementation
                    defReferenceEncoding = "D" + symbol + "." + methodName + ",1";
                } else {
                    // typed implementation
                    defReferenceEncoding = "S" + captured.getCanonicalTypeName() + "." + methodName + ",1";
                }
                valueType = String.class;
                semanticScope.putDecoration(this, new EncodingDecoration(defReferenceEncoding));
            } else {
                valueType = targetType.getTargetType();
                // static case
                if (captured.getType() != def.class) {
                    FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(), getLocation(),
                            targetType.getTargetType(), captured.getCanonicalTypeName(), methodName, 1);
                    semanticScope.putDecoration(this, new ReferenceDecoration(ref));
                }
            }
        }

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}
