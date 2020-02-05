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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents and object instantiation.
 */
public final class ENewObj extends AExpression {

    private final String type;
    private final List<AExpression> arguments;

    private PainlessConstructor constructor;

    public ENewObj(Location location, String type, List<AExpression> arguments) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        actual = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (actual == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        constructor = scriptRoot.getPainlessLookup().lookupPainlessConstructor(actual, arguments.size());

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(actual) + ", <init>/" + arguments.size() + "] not found"));
        }

        scriptRoot.markNonDeterministic(constructor.annotations.containsKey(NonDeterministicAnnotation.class));

        Class<?>[] types = new Class<?>[constructor.typeParameters.size()];
        constructor.typeParameters.toArray(types);

        if (constructor.typeParameters.size() != arguments.size()) {
            throw createError(new IllegalArgumentException(
                    "When calling constructor on type [" + PainlessLookupUtility.typeToCanonicalTypeName(actual) + "] " +
                    "expected [" + constructor.typeParameters.size() + "] arguments, but found [" + arguments.size() + "]."));
        }

        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.expected = types[argument];
            expression.internal = true;
            expression.analyze(scriptRoot, scope);
            arguments.set(argument, expression.cast(scriptRoot, scope));
        }

        statement = true;
    }

    @Override
    NewObjectNode write(ClassNode classNode) {
        NewObjectNode newObjectNode = new NewObjectNode();

        for (AExpression argument : arguments) {
            newObjectNode.addArgumentNode(argument.write(classNode));
        }

        newObjectNode.setLocation(location);
        newObjectNode.setExpressionType(actual);
        newObjectNode.setRead(read);
        newObjectNode.setConstructor(constructor);

        return newObjectNode;
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, type);
    }
}
