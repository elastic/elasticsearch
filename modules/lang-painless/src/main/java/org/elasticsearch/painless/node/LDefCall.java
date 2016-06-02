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
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;

/**
 * Represents a method call made on a def type. (Internal only.)
 */
final class LDefCall extends ALink implements IDefLink {

    final String name;
    final List<AExpression> arguments;

    LDefCall(int line, int offset, String location, String name, List<AExpression> arguments) {
        super(line, offset, location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    ALink analyze(Variables variables) {
        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.internal = true;
            expression.analyze(variables);
            expression.expected = expression.actual;
            arguments.set(argument, expression.cast(variables));
        }

        statement = true;
        after = Definition.DEF_TYPE;

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(offset);
        StringBuilder signature = new StringBuilder();

        signature.append('(');
        // first parameter is the receiver, we never know its type: always Object
        signature.append(Definition.DEF_TYPE.type.getDescriptor());

        for (AExpression argument : arguments) {
            signature.append(argument.actual.type.getDescriptor());
            argument.write(writer);
        }

        signature.append(')');
        // return value
        signature.append(after.type.getDescriptor());

        writer.invokeDynamic(name, signature.toString(), DEF_BOOTSTRAP_HANDLE, (Object)DefBootstrap.METHOD_CALL);
    }

    @Override
    void store(MethodWriter writer) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}
