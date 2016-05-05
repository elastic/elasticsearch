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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.Label;
import org.objectweb.asm.commons.GeneratorAdapter;

public class EConditional extends AExpression {
    protected AExpression condition;
    protected AExpression left;
    protected AExpression right;

    public EConditional(final String location, final AExpression condition, final AExpression left, final AExpression right) {
        super(location);

        this.condition = condition;
        this.left = left;
        this.right = right;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        condition.expected = definition.booleanType;
        condition.analyze(settings, definition, variables);
        condition = condition.cast(definition);

        if (condition.constant != null) {
            throw new IllegalArgumentException(error("Extraneous conditional statement."));
        }

        left.expected = expected;
        right.expected = expected;
        actual = expected;

        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (expected == null) {
            final Type promote = Caster.promoteConditional(definition, left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        left = left.cast(definition);
        right = right.cast(definition);

        typesafe = left.typesafe && right.typesafe;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        final Label localfals = new Label();
        final Label end = new Label();

        condition.fals = localfals;
        left.tru = right.tru = tru;
        left.fals = right.fals = fals;

        condition.write(settings, definition, adapter);
        left.write(settings, definition, adapter);
        adapter.goTo(end);
        adapter.mark(localfals);
        right.write(settings, definition, adapter);
        adapter.mark(end);
    }
}
