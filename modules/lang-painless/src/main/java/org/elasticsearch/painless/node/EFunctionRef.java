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
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Type;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a function reference.
 */
public final class EFunctionRef extends AExpression implements ILambda {
    private final String type;
    private final String call;

    private FunctionRef ref;
    private String defPointer;

    public EFunctionRef(Location location, String type, String call) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.call = Objects.requireNonNull(call);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        // do nothing
    }

    @Override
    void extractVariables(Set<String> variables) {
        // do nothing
    }

    @Override
    void analyze(Locals locals) {
        if (expected == null) {
            ref = null;
            actual = String.class;
            defPointer = "S" + type + "." + call + ",0";
        } else {
            defPointer = null;
            ref = FunctionRef.create(locals.getPainlessLookup(), locals.getMethods(), location, expected, type, call, 0);
            actual = expected;
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        if (ref != null) {
            writer.writeDebugInfo(location);
            writer.invokeLambdaCall(ref);
        } else {
            // TODO: don't do this: its just to cutover :)
            writer.push((String)null);
        }
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public Type[] getCaptures() {
        return new Type[0]; // no captures
    }

    @Override
    public String toString() {
        return singleLineToString(type, call);
    }
}
