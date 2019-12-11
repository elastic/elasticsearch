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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a map load/store shortcut. (Internal only.)
 */
final class PSubMapShortcut extends AStoreable {

    private final Class<?> targetClass;
    private AExpression index;

    private PainlessMethod getter;
    private PainlessMethod setter;

    PSubMapShortcut(Location location, Class<?> targetClass, AExpression index) {
        super(location);

        this.targetClass = Objects.requireNonNull(targetClass);
        this.index = Objects.requireNonNull(index);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);

        getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetClass, false, "get", 1);
        setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetClass, false, "put", 2);

        if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1)) {
            throw createError(new IllegalArgumentException("Illegal map get shortcut for type [" + canonicalClassName + "]."));
        }

        if (setter != null && setter.typeParameters.size() != 2) {
            throw createError(new IllegalArgumentException("Illegal map set shortcut for type [" + canonicalClassName + "]."));
        }

        if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0)) ||
                !getter.returnType.equals(setter.typeParameters.get(1)))) {
            throw createError(new IllegalArgumentException("Shortcut argument types must match."));
        }

        if ((read || write) && (!read || getter != null) && (!write || setter != null)) {
            index.expected = setter != null ? setter.typeParameters.get(0) : getter.typeParameters.get(0);
            index.analyze(scriptRoot, locals);
            index = index.cast(scriptRoot, locals);

            actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
        } else {
            throw createError(new IllegalArgumentException("Illegal map shortcut for type [" + canonicalClassName + "]."));
        }
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        index.write(classWriter, methodWriter, globals);

        methodWriter.writeDebugInfo(location);
        methodWriter.invokeMethodCall(getter);

        if (getter.returnType != getter.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getter.returnType));
        }
    }

    @Override
    int accessElementCount() {
        return 2;
    }

    @Override
    boolean isDefOptimized() {
        return false;
    }

    @Override
    void updateActual(Class<?> actual) {
        throw new IllegalArgumentException("Illegal tree structure.");
    }

    @Override
    void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        index.write(classWriter, methodWriter, globals);
    }

    @Override
    void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);
        methodWriter.invokeMethodCall(getter);

        if (getter.returnType != getter.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getter.returnType));
        }
    }

    @Override
    void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);
        methodWriter.invokeMethodCall(setter);
        methodWriter.writePop(MethodWriter.getType(setter.returnType).getSize());
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}
