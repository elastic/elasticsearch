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
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.MethodNameDecoration;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ReferenceDecoration;
import org.elasticsearch.painless.symbol.Decorations.ReturnType;
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
public class ENewArrayFunctionRef extends AExpression {

    private final String canonicalTypeName;

    public ENewArrayFunctionRef(int identifier, Location location, String canonicalTypeName) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitNewArrayFunctionRef(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "cannot assign a value to new array function reference with target type [ + " + canonicalTypeName  + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: new array function reference with target type [" + canonicalTypeName + "] not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        TargetType targetType = semanticScope.getDecoration(this, TargetType.class);

        Class<?> valueType;
        Class<?> clazz = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);
        semanticScope.putDecoration(this, new ReturnType(clazz));

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        String name = scriptScope.getNextSyntheticName("newarray");
        scriptScope.getFunctionTable().addFunction(name, clazz, Collections.singletonList(int.class), true, true);
        semanticScope.putDecoration(this, new MethodNameDecoration(name));

        if (targetType == null) {
            String defReferenceEncoding = "Sthis." + name + ",0";
            valueType = String.class;
            scriptScope.putDecoration(this, new EncodingDecoration(defReferenceEncoding));
        } else {
            FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                    getLocation(), targetType.getTargetType(), "this", name, 0);
            valueType = targetType.getTargetType();
            semanticScope.putDecoration(this, new ReferenceDecoration(ref));
        }

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}
