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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * Represents a set of statements as a branch of control-flow.
 */
public final class SBlock extends AStatement {

    final List<AStatement> statements;

    public SBlock(Location location, List<AStatement> statements) {
        super(location);

        this.statements = Collections.unmodifiableList(statements);
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AStatement statement : statements) {
            statement.extractVariables(variables);
        }
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("A block must contain at least one statement."));
        }

        AStatement last = statements.get(statements.size() - 1);

        for (AStatement statement : statements) {
            // Note that we do not need to check after the last statement because
            // there is no statement that can be unreachable after the last.
            if (allEscape) {
                throw createError(new IllegalArgumentException("Unreachable statement."));
            }

            statement.inLoop = inLoop;
            statement.lastSource = lastSource && statement == last;
            statement.lastLoop = (beginLoop || lastLoop) && statement == last;
            statement.analyze(scriptRoot, locals);

            methodEscape = statement.methodEscape;
            loopEscape = statement.loopEscape;
            allEscape = statement.allEscape;
            anyContinue |= statement.anyContinue;
            anyBreak |= statement.anyBreak;
            statementCount += statement.statementCount;
        }
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        for (AStatement statement : statements) {
            statement.continu = continu;
            statement.brake = brake;
            statement.write(classWriter, methodWriter, globals);
        }
    }

    @Override
    public String toString() {
        return multilineToString(emptyList(), statements);
    }
}
