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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Represents the try block as part of a try-catch block.
 */
public final class STry extends AStatement {

    private final SBlock block;
    private final List<SCatch> catches;

    public STry(Location location, SBlock block, List<SCatch> catches) {
        super(location);

        this.block = block;
        this.catches = Collections.unmodifiableList(catches);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        if (block != null) {
            block.storeSettings(settings);
        }

        for (SCatch ctch : catches) {
            ctch.storeSettings(settings);
        }
    }

    @Override
    void extractVariables(Set<String> variables) {
        if (block != null) {
            block.extractVariables(variables);
        }
        for (SCatch expr : catches) {
            expr.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        if (block == null) {
            throw createError(new IllegalArgumentException("Extraneous try statement."));
        }

        block.lastSource = lastSource;
        block.inLoop = inLoop;
        block.lastLoop = lastLoop;

        block.analyze(Locals.newLocalScope(locals));

        methodEscape = block.methodEscape;
        loopEscape = block.loopEscape;
        allEscape = block.allEscape;
        anyContinue = block.anyContinue;
        anyBreak = block.anyBreak;

        int statementCount = 0;

        for (SCatch catc : catches) {
            catc.lastSource = lastSource;
            catc.inLoop = inLoop;
            catc.lastLoop = lastLoop;

            catc.analyze(Locals.newLocalScope(locals));

            methodEscape &= catc.methodEscape;
            loopEscape &= catc.loopEscape;
            allEscape &= catc.allEscape;
            anyContinue |= catc.anyContinue;
            anyBreak |= catc.anyBreak;

            statementCount = Math.max(statementCount, catc.statementCount);
        }

        this.statementCount = block.statementCount + statementCount;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeStatementOffset(location);

        Label begin = new Label();
        Label end = new Label();
        Label exception = new Label();

        writer.mark(begin);

        block.continu = continu;
        block.brake = brake;
        block.write(writer, globals);

        if (!block.allEscape) {
            writer.goTo(exception);
        }

        writer.mark(end);

        for (SCatch catc : catches) {
            catc.begin = begin;
            catc.end = end;
            catc.exception = catches.size() > 1 ? exception : null;
            catc.write(writer, globals);
        }

        if (!block.allEscape || catches.size() > 1) {
            writer.mark(exception);
        }
    }

    @Override
    public String toString() {
        return multilineToString(singleton(block), catches);
    }
}
