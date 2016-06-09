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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents an if block.
 */
public final class SIf extends AStatement {

    AExpression condition;
    final SBlock ifblock;

    public SIf(Location location, AExpression condition, SBlock ifblock) {
        super(location);

        this.condition = condition;
        this.ifblock = ifblock;
    }

    @Override
    void analyze(Locals locals) {
        condition.expected = Definition.BOOLEAN_TYPE;
        condition.analyze(locals);
        condition = condition.cast(locals);

        if (condition.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        if (ifblock == null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        ifblock.lastSource = lastSource;
        ifblock.inLoop = inLoop;
        ifblock.lastLoop = lastLoop;

        locals.incrementScope();
        ifblock.analyze(locals);
        locals.decrementScope();

        anyContinue = ifblock.anyContinue;
        anyBreak = ifblock.anyBreak;
        statementCount = ifblock.statementCount;
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeStatementOffset(location);

        Label fals = new Label();

        condition.fals = fals;
        condition.write(writer);

        ifblock.continu = continu;
        ifblock.brake = brake;
        ifblock.write(writer);

        writer.mark(fals);
    }
}
