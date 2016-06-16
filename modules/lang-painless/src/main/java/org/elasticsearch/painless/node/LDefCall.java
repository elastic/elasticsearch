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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;

/**
 * Represents a method call made on a def type. (Internal only.)
 */
final class LDefCall extends ALink implements IDefLink {

    final String name;
    final List<AExpression> arguments;
    long recipe;
    List<String> pointers = new ArrayList<>();

    LDefCall(Location location, String name, List<AExpression> arguments) {
        super(location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    ALink analyze(Locals locals) {
        if (arguments.size() > 63) {
            // technically, the limitation is just methods with > 63 params, containing method references.
            // this is because we are lazy and use a long as a bitset. we can always change to a "string" if need be.
            // but NEED NOT BE. nothing with this many parameters is in the whitelist and we do not support varargs.
            throw new UnsupportedOperationException("methods with > 63 arguments are currently not supported");
        }

        recipe = 0;
        int totalCaptures = 0;
        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.internal = true;
            expression.analyze(locals);

            if (expression instanceof EFunctionRef) {
                pointers.add(((EFunctionRef)expression).defPointer);
                recipe |= (1L << (argument + totalCaptures)); // mark argument as deferred reference
            } else if (expression instanceof ECapturingFunctionRef) {
                pointers.add(((ECapturingFunctionRef)expression).defPointer);
                recipe |= (1L << (argument + totalCaptures)); // mark argument as deferred reference
                totalCaptures++;
            }

            expression.expected = expression.actual;
            arguments.set(argument, expression.cast(locals));
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
        writer.writeDebugInfo(location);

        StringBuilder signature = new StringBuilder();

        signature.append('(');
        // first parameter is the receiver, we never know its type: always Object
        signature.append(Definition.DEF_TYPE.type.getDescriptor());

        for (AExpression argument : arguments) {
            signature.append(argument.actual.type.getDescriptor());
            if (argument instanceof ECapturingFunctionRef) {
                ECapturingFunctionRef capturingRef = (ECapturingFunctionRef) argument;
                signature.append(capturingRef.captured.type.type.getDescriptor());
            }
            argument.write(writer);
        }

        signature.append(')');
        // return value
        signature.append(after.type.getDescriptor());

        List<Object> args = new ArrayList<>();
        args.add(DefBootstrap.METHOD_CALL);
        args.add(recipe);
        args.addAll(pointers);
        writer.invokeDynamic(name, signature.toString(), DEF_BOOTSTRAP_HANDLE, args.toArray());
    }

    @Override
    void store(MethodWriter writer) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}
