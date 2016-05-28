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

    public EConditional(int line, int offset, String location, AExpression condition, AExpression left, AExpression right) {
        super(line, offset, location);

        this.condition = condition;
        this.left = left;
        this.right = right;
    }

    @Override
    void analyze(Variables variables) {
        condition.expected = Definition.BOOLEAN_TYPE;
        condition.analyze(variables);
        condition = condition.cast(variables);

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

        left.analyze(variables);
        right.analyze(variables);

        if (expected == null) {
            final Type promote = AnalyzerCaster.promoteConditional(left.actual, right.actual, left.constant, right.constant);

            left.expected = promote;
            right.expected = promote;
            actual = promote;
        }

        left = left.cast(variables);
        right = right.cast(variables);
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeDebugInfo(offset);

        Label localfals = new Label();
        Label end = new Label();

        condition.fals = localfals;
        left.tru = right.tru = tru;
        left.fals = right.fals = fals;

        condition.write(writer);
        left.write(writer);
        writer.goTo(end);
        writer.mark(localfals);
        right.write(writer);
        writer.mark(end);
    }
}
