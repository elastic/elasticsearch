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
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.StandardConstant;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a string constant.
 */
public class EString extends AExpression {

    private String string;

    public EString(int identifier, Location location, String string) {
        super(identifier, location);

        this.string = Objects.requireNonNull(string);
    }

    public String getString() {
        return string;
    }

    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitString(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to string constant [" + string + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException("not a statement: string constant [" + string + "] not used"));
        }

        semanticScope.putDecoration(this, new ValueType(String.class));
        semanticScope.putDecoration(this, new StandardConstant(string));
    }
}
