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
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a method call and defers to a child subnode.
 */
public final class PCallInvoke extends AExpression {

    private final String name;
    private final boolean nullSafe;
    private final List<AExpression> arguments;

    private AExpression sub = null;

    public PCallInvoke(Location location, AExpression prefix, String name, boolean nullSafe, List<AExpression> arguments) {
        super(location, prefix);

        this.name = Objects.requireNonNull(name);
        this.nullSafe = nullSafe;
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        prefix.analyze(scriptRoot, scope);
        prefix.expected = prefix.actual;
        prefix = prefix.cast(scriptRoot, scope);

        if (prefix.actual == def.class) {
            sub = new PSubDefCall(location, name, arguments);
        } else {
            PainlessMethod method =
                    scriptRoot.getPainlessLookup().lookupPainlessMethod(prefix.actual, prefix instanceof EStatic, name, arguments.size());

            if (method == null) {
                throw createError(new IllegalArgumentException(
                        "method [" + typeToCanonicalTypeName(prefix.actual) + ", " + name + "/" + arguments.size() + "] not found"));
            }

            scriptRoot.markNonDeterministic(method.annotations.containsKey(NonDeterministicAnnotation.class));

            sub = new PSubCallInvoke(location, method, prefix.actual, arguments);
        }

        if (nullSafe) {
            sub = new PSubNullSafeCallInvoke(location, sub);
        }

        sub.expected = expected;
        sub.explicit = explicit;
        sub.analyze(scriptRoot, scope);
        actual = sub.actual;

        statement = true;
    }

    @Override
    CallNode write(ClassNode classNode) {
        CallNode callNode = new CallNode();

        callNode.setLeftNode(prefix.write(classNode));
        callNode.setRightNode(sub.write(classNode));

        callNode.setLocation(location);
        callNode.setExpressionType(actual);

        return callNode;
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, name);
    }
}
