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
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class SDo extends Statement {
    protected final Statement block;
    protected Expression condition;

    public SDo(final String location, final Statement block, final Expression condition) {
        super(location);

        this.condition = condition;
        this.block = block;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        variables.incrementScope();

        block.beginLoop = true;
        block.inLoop = true;

        block.analyze(settings, definition, variables);

        if (block.loopEscape && !block.anyContinue) {
            throw new IllegalArgumentException(error("Extraneous do while loop."));
        }

        condition.expected = definition.booleanType;
        condition.analyze(settings, definition, variables);
        condition = condition.cast(definition);

        if (condition.constant != null) {
            final boolean continuous = (boolean)condition.constant;

            if (!continuous) {
                throw new IllegalArgumentException(error("Extraneous do while loop."));
            }

            if (!block.anyBreak) {
                methodEscape = true;
                allEscape = true;
            }
        }

        statementCount = 1;

        variables.decrementScope();
    }

    @Override
    protected void write(final GeneratorAdapter adapter) {

    }
}
