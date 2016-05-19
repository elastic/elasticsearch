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
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents an if block.
 */
public final class SIf extends AStatement {

    AExpression condition;
    final SBlock ifblock;

    public SIf(final int line, final String location,
               final AExpression condition, final SBlock ifblock) {
        super(line, location);

        this.condition = condition;
        this.ifblock = ifblock;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        condition.expected = definition.booleanType;
        condition.analyze(settings, definition, variables);
        condition = condition.cast(settings, definition, variables);

        if (condition.constant != null) {
            throw new IllegalArgumentException(error("Extraneous if statement."));
        }

        if (ifblock == null) {
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
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        writeDebugInfo(adapter);
        final Label fals = new Label();

        condition.fals = fals;
        condition.write(settings, definition, adapter);

        ifblock.continu = continu;
        ifblock.brake = brake;
        ifblock.write(settings, definition, adapter);

        adapter.mark(fals);
    }
}
