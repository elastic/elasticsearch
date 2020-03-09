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
import org.elasticsearch.painless.ir.DotSubNode;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.lang.reflect.Modifier;
import java.util.Objects;

/**
 * Represents a field load/store.
 */
final class PSubField extends AStoreable {

    private final PainlessField field;

    PSubField(Location location, PainlessField field) {
        super(location);

        this.field = Objects.requireNonNull(field);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
         if (write && Modifier.isFinal(field.javaField.getModifiers())) {
             throw createError(new IllegalArgumentException("Cannot write to read-only field [" + field.javaField.getName() + "] " +
                     "for type [" + PainlessLookupUtility.typeToCanonicalTypeName(field.javaField.getDeclaringClass()) + "]."));
         }

        actual = field.typeParameter;
    }

    @Override
    DotSubNode write(ClassNode classNode) {
        DotSubNode dotSubNode = new DotSubNode();

        dotSubNode.setLocation(location);
        dotSubNode.setExpressionType(actual);
        dotSubNode.setField(field);

        return dotSubNode;
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
    public String toString() {
        return singleLineToString(prefix, field.javaField.getName());
    }
}
