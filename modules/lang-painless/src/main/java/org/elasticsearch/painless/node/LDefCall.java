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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

        this.name = Objects.requireNonNull(name);
        this.arguments = Objects.requireNonNull(arguments);
    }
    
    @Override
    void extractVariables(Set<String> variables) {
        for (AExpression argument : arguments) {
            argument.extractVariables(variables);
        }
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

            if (expression instanceof ILambda) {
                ILambda lambda = (ILambda) expression;
                pointers.add(lambda.getPointer());
                recipe |= (1L << (argument + totalCaptures)); // mark argument as deferred reference
                totalCaptures += lambda.getCaptureCount();
            }

            expression.expected = expression.actual;
            arguments.set(argument, expression.cast(locals));
        }

        statement = true;
        after = Definition.DEF_TYPE;

        return this;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        StringBuilder signature = new StringBuilder();

        signature.append('(');
        // first parameter is the receiver, we never know its type: always Object
        signature.append(Definition.DEF_TYPE.type.getDescriptor());

        for (AExpression argument : arguments) {
            signature.append(argument.actual.type.getDescriptor());
            if (argument instanceof ILambda) {
                ILambda lambda = (ILambda) argument;
                for (Type capture : lambda.getCaptures()) {
                    signature.append(capture.getDescriptor());
                }
            }
            argument.write(writer, globals);
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
    void store(MethodWriter writer, Globals globals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}
