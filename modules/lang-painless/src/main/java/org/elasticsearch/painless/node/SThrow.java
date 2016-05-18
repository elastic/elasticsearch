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
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a throw statement.
 */
public final class SThrow extends AStatement {

    AExpression expression;

    public SThrow(final int line, final String location, final AExpression expression) {
        super(line, location);

        this.expression = expression;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        expression.expected = definition.exceptionType;
        expression.analyze(settings, definition, variables);
        expression = expression.cast(settings, definition, variables);

        methodEscape = true;
        loopEscape = true;
        allEscape = true;
        statementCount = 1;
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        writeDebugInfo(adapter);
        expression.write(settings, definition, adapter);
        adapter.throwException();
    }
}
