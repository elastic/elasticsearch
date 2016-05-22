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

    LDefCall(int line, String location, String name, List<AExpression> arguments) {
        super(line, location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    ALink analyze(Variables variables) {
        for (int argument = 0; argument < arguments.size(); ++argument) {
            final AExpression expression = arguments.get(argument);

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
    void write(MethodWriter adapter) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter adapter) {
        final StringBuilder signature = new StringBuilder();

        signature.append('(');
        // first parameter is the receiver, we never know its type: always Object
        signature.append(Definition.DEF_TYPE.type.getDescriptor());

        // TODO: remove our explicit conversions and feed more type information for return value,
        // it can avoid some unnecessary boxing etc.
        for (final AExpression argument : arguments) {
            signature.append(argument.actual.type.getDescriptor());
            argument.write(adapter);
        }

        signature.append(')');
        // return value
        signature.append(after.type.getDescriptor());

        adapter.invokeDynamic(name, signature.toString(), DEF_BOOTSTRAP_HANDLE, DefBootstrap.METHOD_CALL);
    }

    @Override
    void store(MethodWriter adapter) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}
