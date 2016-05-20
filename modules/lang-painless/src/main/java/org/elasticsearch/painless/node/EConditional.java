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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

/**
 * Respresents a conditional expression.
 */
public final class EConditional extends AExpression {

    AExpression condition;
    AExpression left;
    AExpression right;

    public EConditional(final int line, final String location,
                        final AExpression condition, final AExpression left, final AExpression right) {
        super(line, location);

        this.condition = condition;
        this.left = left;
        this.right = right;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        condition.expected = definition.getType("boolean");
        condition.analyze(settings, definition, variables);
        condition = condition.cast(settings, definition, variables);

        if (condition.constant != null) {
            throw new IllegalArgumentException(error("Extraneous conditional statement."));
        }

        left.expected = expected;
        left.explicit = explicit;
        left.internal = internal;
        right.expected = expected;
        right.explicit = explicit;
        right.internal = internal;
        actual = expected;

        left.analyze(settings, definition, variables);
        right.analyze(settings, definition, variables);

        if (expected == null) {
            final Type promote = AnalyzerCaster.promoteConditional(definition, left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        left = left.cast(settings, definition, variables);
        right = right.cast(settings, definition, variables);
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
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
