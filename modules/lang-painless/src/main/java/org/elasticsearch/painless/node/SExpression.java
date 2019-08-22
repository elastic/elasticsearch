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

import java.util.Objects;
import java.util.Set;

/**
 * Represents the top-level node for an expression as a statement.
 */
public final class SExpression extends AStatement {

    public SExpression(Location location, AExpression expression) {
        super(location);

        children.add(Objects.requireNonNull(expression));
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        children.get(0).storeSettings(settings);
    }

    @Override
    void extractVariables(Set<String> variables) {
        children.get(0).extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        AExpression expression = (AExpression)children.get(0);

        Class<?> rtnType = locals.getReturnType();
        boolean isVoid = rtnType == void.class;

        expression.read = lastSource && !isVoid;
        expression.analyze(locals);

        if (!lastSource && !expression.statement) {
            throw createError(new IllegalArgumentException("Not a statement."));
        }

        boolean rtn = lastSource && !isVoid && expression.actual != void.class;

        expression.expected = rtn ? rtnType : expression.actual;
        expression.internal = rtn;
        children.set(0, expression.cast(locals));

        methodEscape = rtn;
        loopEscape = rtn;
        allEscape = rtn;
        statementCount = 1;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        AExpression expression = (AExpression)children.get(0);

        writer.writeStatementOffset(location);
        expression.write(writer, globals);

        if (methodEscape) {
            writer.returnValue();
        } else {
            writer.writePop(MethodWriter.getType(expression.expected).getSize());
        }
    }

    @Override
    public String toString() {
        return singleLineToString(children.get(0));
    }
}
