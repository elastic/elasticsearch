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

import java.util.Set;

/**
 * Represents a break statement.
 */
public final class SBreak extends AStatement {

    public SBreak(Location location) {
        super(location);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        // do nothing
    }

    @Override
    void extractVariables(Set<String> variables) {
        // Do nothing.
    }

    @Override
    void analyze(Locals locals) {
        if (!inLoop) {
            throw createError(new IllegalArgumentException("Break statement outside of a loop."));
        }

        loopEscape = true;
        allEscape = true;
        anyBreak = true;
        statementCount = 1;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.goTo(brake);
    }

    @Override
    public String toString() {
        return singleLineToString();
    }
}
