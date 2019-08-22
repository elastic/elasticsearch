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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Represents an if/else block.
 */
public final class SIfElse extends AStatement {

    public SIfElse(Location location, AExpression condition, SBlock ifblock, SBlock elseblock) {
        super(location);

        children.add(Objects.requireNonNull(condition));
        children.add(ifblock);
        children.add(elseblock);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        children.get(0).storeSettings(settings);

        if (children.get(1) != null) {
            children.get(1).storeSettings(settings);
        }

        if (children.get(2) != null) {
            children.get(2).storeSettings(settings);
        }
    }

    @Override
    void extractVariables(Set<String> variables) {
        children.get(0).extractVariables(variables);

        if (children.get(1) != null) {
            children.get(1).extractVariables(variables);
        }

        if (children.get(2) != null) {
            children.get(2).extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        AExpression condition = (AExpression)children.get(0);
        SBlock ifblock = (SBlock)children.get(1);
        SBlock elseblock = (SBlock)children.get(2);

        condition.expected = boolean.class;
        condition.analyze(locals);
        children.set(0, condition = condition.cast(locals));

        if (condition.constant != null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        if (ifblock == null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        ifblock.lastSource = lastSource;
        ifblock.inLoop = inLoop;
        ifblock.lastLoop = lastLoop;

        ifblock.analyze(Locals.newLocalScope(locals));

        anyContinue = ifblock.anyContinue;
        anyBreak = ifblock.anyBreak;
        statementCount = ifblock.statementCount;

        if (elseblock == null) {
            throw createError(new IllegalArgumentException("Extraneous else statement."));
        }

        elseblock.lastSource = lastSource;
        elseblock.inLoop = inLoop;
        elseblock.lastLoop = lastLoop;

        elseblock.analyze(Locals.newLocalScope(locals));

        methodEscape = ifblock.methodEscape && elseblock.methodEscape;
        loopEscape = ifblock.loopEscape && elseblock.loopEscape;
        allEscape = ifblock.allEscape && elseblock.allEscape;
        anyContinue |= elseblock.anyContinue;
        anyBreak |= elseblock.anyBreak;
        statementCount = Math.max(ifblock.statementCount, elseblock.statementCount);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        SBlock ifblock = (SBlock)children.get(1);
        SBlock elseblock = (SBlock)children.get(2);

        writer.writeStatementOffset(location);

        Label fals = new Label();
        Label end = new Label();

        children.get(0).write(writer, globals);
        writer.ifZCmp(Opcodes.IFEQ, fals);

        ifblock.continu = continu;
        ifblock.brake = brake;
        ifblock.write(writer, globals);

        if (!ifblock.allEscape) {
            writer.goTo(end);
        }

        writer.mark(fals);

        elseblock.continu = continu;
        elseblock.brake = brake;
        elseblock.write(writer, globals);

        writer.mark(end);
    }

    @Override
    public String toString() {
        return multilineToString(singleton(children.get(0)), Arrays.asList(children.get(1), children.get(2)));
    }
}
