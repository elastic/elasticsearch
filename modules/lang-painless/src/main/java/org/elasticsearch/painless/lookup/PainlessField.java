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

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.util.Objects;

public final class PainlessField {

    public final Field javaField;
    public final Class<?> typeParameter;

    public final MethodHandle getterMethodHandle;
    public final MethodHandle setterMethodHandle;

    PainlessField(Field javaField, Class<?> typeParameter, MethodHandle getterMethodHandle, MethodHandle setterMethodHandle) {
        this.javaField = javaField;
        this.typeParameter = typeParameter;

        this.getterMethodHandle = getterMethodHandle;
        this.setterMethodHandle = setterMethodHandle;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessField that = (PainlessField)object;

        return Objects.equals(javaField, that.javaField) &&
                Objects.equals(typeParameter, that.typeParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaField, typeParameter);
    }
}
