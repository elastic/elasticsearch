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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

public class PainlessClassBinding {

    public final Constructor<?> javaConstructor;
    public final Method javaMethod;

    public final Class<?> returnType;
    public final List<Class<?>> typeParameters;

    PainlessClassBinding(Constructor<?> javaConstructor, Method javaMethod, Class<?> returnType, List<Class<?>> typeParameters) {
        this.javaConstructor = javaConstructor;
        this.javaMethod = javaMethod;

        this.returnType = returnType;
        this.typeParameters = typeParameters;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessClassBinding that = (PainlessClassBinding)object;

        return Objects.equals(javaConstructor, that.javaConstructor) &&
                Objects.equals(javaMethod, that.javaMethod) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(typeParameters, that.typeParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaConstructor, javaMethod, returnType, typeParameters);
    }
}
