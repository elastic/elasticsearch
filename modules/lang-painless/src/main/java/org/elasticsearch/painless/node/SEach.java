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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a for-each loop and defers to subnodes depending on type.
 */
public class SEach extends AStatement {

    private final String type;
    private final String name;
    private AExpression expression;
    private final SBlock block;

    private AStatement sub = null;

    public SEach(Location location, String type, String name, AExpression expression, SBlock block) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.expression = Objects.requireNonNull(expression);
        this.block = block;
    }

    @Override
    void extractVariables(Set<String> variables) {
        variables.add(name);

        expression.extractVariables(variables);

        if (block != null) {
            block.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        expression.analyze(locals);
        expression.expected = expression.actual;
        expression = expression.cast(locals);

        final Type type;

        try {
            type = locals.getDefinition().getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        locals = Locals.newLocalScope(locals);
        Variable variable = locals.addVariable(location, type, name, true);

        if (expression.actual.dimensions > 0) {
            sub = new SSubEachArray(location, variable, expression, block);
        } else if (expression.actual.dynamic || Iterable.class.isAssignableFrom(expression.actual.clazz)) {
            sub = new SSubEachIterable(location, variable, expression, block);
        } else {
            throw createError(new IllegalArgumentException("Illegal for each type [" + expression.actual.name + "]."));
        }

        sub.analyze(locals);

        if (block == null) {
            throw createError(new IllegalArgumentException("Extraneous for each loop."));
        }

        block.beginLoop = true;
        block.inLoop = true;
        block.analyze(locals);
        block.statementCount = Math.max(1, block.statementCount);

        if (block.loopEscape && !block.anyContinue) {
            throw createError(new IllegalArgumentException("Extraneous for loop."));
        }

        statementCount = 1;

        if (locals.hasVariable(Locals.LOOP)) {
            sub.loopCounter = locals.getVariable(location, Locals.LOOP);
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        sub.write(writer, globals);
    }

    @Override
    public String toString() {
        return singleLineToString(type, name, expression, block);
    }
}
