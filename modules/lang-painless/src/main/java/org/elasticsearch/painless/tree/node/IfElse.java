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
import org.elasticsearch.painless.tree.utility.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class IfElse extends Statement {
    protected Expression condition;
    protected final Statement ifblock;
    protected final Statement elseblock;

    public IfElse(final String location, final Expression condition, final Statement ifblock, final Statement elseblock) {
        super(location);

        this.condition = condition;
        this.ifblock = ifblock;
        this.elseblock = elseblock;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        condition.expected = definition.booleanType;
        condition.analyze(settings, definition, variables);
        condition = condition.cast(definition);

        if (condition.constant != null) {
            throw new IllegalArgumentException(error("Extraneous if statement."));
        }

        ifblock.lastSource = lastSource;
        ifblock.inLoop = inLoop;
        ifblock.lastLoop = lastLoop;

        variables.incrementScope();
        ifblock.analyze(settings, definition, variables);
        variables.decrementScope();

        anyContinue = ifblock.anyContinue;
        anyBreak = ifblock.anyBreak;
        statementCount = ifblock.statementCount;

        if (elseblock != null) {
            elseblock.lastSource = lastSource;
            elseblock.inLoop = inLoop;
            elseblock.lastLoop = lastLoop;

            variables.incrementScope();
            elseblock.analyze(settings, definition, variables);
            variables.decrementScope();

            methodEscape = ifblock.methodEscape && elseblock.methodEscape;
            loopEscape = ifblock.loopEscape && elseblock.loopEscape;
            allEscape = ifblock.allEscape && elseblock.allEscape;
            anyContinue |= elseblock.anyContinue;
            anyBreak |= elseblock.anyBreak;
            statementCount = Math.max(ifblock.statementCount, elseblock.statementCount);
        }
    }

    protected void write(final GeneratorAdapter adapter) {

    }
}
