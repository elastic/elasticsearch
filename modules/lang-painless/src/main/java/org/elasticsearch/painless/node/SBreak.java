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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

/**
 * Represents a break statement.
 */
public class SBreak extends AStatement {

    public SBreak(Location location) {
        super(location);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        if (input.inLoop == false) {
            throw createError(new IllegalArgumentException("Break statement outside of a loop."));
        }

        output.loopEscape = true;
        output.allEscape = true;
        output.anyBreak = true;
        output.statementCount = 1;

        BreakNode breakNode = new BreakNode();
        breakNode.setLocation(location);

        output.statementNode = breakNode;

        return output;
    }
}
