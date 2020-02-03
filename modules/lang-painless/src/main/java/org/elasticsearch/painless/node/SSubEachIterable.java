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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.Scope.Variable;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a for-each loop for iterables.
 */
final class SSubEachIterable extends AStatement {

    private AExpression expression;
    private final SBlock block;
    private final Variable variable;

    private PainlessCast cast = null;
    private Variable iterator = null;
    private PainlessMethod method = null;

    SSubEachIterable(Location location, Variable variable, AExpression expression, SBlock block) {
        super(location);

        this.variable = Objects.requireNonNull(variable);
        this.expression = Objects.requireNonNull(expression);
        this.block = block;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        // We must store the iterator as a variable for securing a slot on the stack, and
        // also add the location offset to make the name unique in case of nested for each loops.
        iterator = scope.defineInternalVariable(location, Iterator.class, "itr" + location.getOffset(), true);

        if (expression.actual == def.class) {
            method = null;
        } else {
            method = scriptRoot.getPainlessLookup().lookupPainlessMethod(expression.actual, false, "iterator", 0);

            if (method == null) {
                    throw createError(new IllegalArgumentException(
                            "method [" + typeToCanonicalTypeName(expression.actual) + ", iterator/0] not found"));
            }
        }

        cast = AnalyzerCaster.getLegalCast(location, def.class, variable.getType(), true, true);
    }

    @Override
    ForEachSubIterableNode write(ClassNode classNode) {
        ForEachSubIterableNode forEachSubIterableNode = new ForEachSubIterableNode();

        forEachSubIterableNode.setConditionNode(expression.write(classNode));
        forEachSubIterableNode.setBlockNode(block.write(classNode));

        forEachSubIterableNode.setLocation(location);
        forEachSubIterableNode.setVariableType(variable.getType());
        forEachSubIterableNode.setVariableName(variable.getName());
        forEachSubIterableNode.setCast(cast);
        forEachSubIterableNode.setIteratorType(iterator.getType());
        forEachSubIterableNode.setIteratorName(iterator.getName());
        forEachSubIterableNode.setMethod(method);
        forEachSubIterableNode.setContinuous(false);

        return forEachSubIterableNode;
    }

    @Override
    public String toString() {
        return singleLineToString(variable.getCanonicalTypeName(), variable.getName(), expression, block);
    }
}
