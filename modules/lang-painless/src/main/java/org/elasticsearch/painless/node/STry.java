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

import java.util.List;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Represents the try block as part of a try-catch block.
 */
public final class STry extends AStatement {

    public STry(Location location, SBlock block, List<SCatch> catches) {
        super(location);

        children.add(block);
        children.addAll(catches);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        for (ANode child : children) {
            child.storeSettings(settings);
        }
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (ANode child : children) {
            child.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        SBlock block = (SBlock)children.get(0);

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

        for (int catchIndex = 1; catchIndex < children.size(); ++catchIndex) {
            SCatch catc = (SCatch)children.get(catchIndex);

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
        SBlock block = (SBlock)children.get(0);

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

        for (int catchIndex = 1; catchIndex < children.size(); ++catchIndex) {
            SCatch catc = (SCatch)children.get(catchIndex);
            catc.begin = begin;
            catc.end = end;
            catc.exception = children.size() > 2 ? exception : null;
            catc.write(writer, globals);
        }

        if (!block.allEscape || children.size() > 2) {
            writer.mark(exception);
        }
    }

    @Override
    public String toString() {
        return multilineToString(singleton(children.get(0)), children.subList(1, children.size()));
    }
}
