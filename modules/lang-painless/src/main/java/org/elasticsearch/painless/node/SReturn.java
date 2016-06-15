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
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a return statement.
 */
public final class SReturn extends AStatement {

    AExpression expression;

    public SReturn(Location location, AExpression expression) {
        super(location);

        this.expression = expression;
    }

    @Override
    void analyze(Locals locals) {
        expression.expected = locals.getReturnType();
        expression.internal = true;
        expression.analyze(locals);
        expression = expression.cast(locals);

        methodEscape = true;
        loopEscape = true;
        allEscape = true;

        statementCount = 1;
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeStatementOffset(location);
        expression.write(writer);
        writer.returnValue();
    }
}
