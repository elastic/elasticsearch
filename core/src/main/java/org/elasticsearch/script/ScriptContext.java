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

package org.elasticsearch.script;

import java.lang.reflect.Method;

/**
 * The information necessary to compile and run a script.
 *
 * A {@link ScriptContext} contains the information related to a single use case and the interfaces
 * and methods necessary for a {@link ScriptEngine} to implement.
 * <p>
 * There are two related classes which must be supplied to construct a {@link ScriptContext}.
 * <p>
 * The <i>FactoryType</i> is a factory class for constructing instances of a script. The
 * {@link ScriptService} returns an instance of <i>FactoryType</i> when compiling a script. This class
 * must be stateless so it is cacheable by the {@link ScriptService}. It must have an abstract method
 * named {@code newInstance} which {@link ScriptEngine} implementations will define.
 * <p>
 * The <i>InstanceType</i> is a class returned by the {@code newInstance} method of the
 * <i>FactoryType</i>. It is an instance of a script and may be stateful. Instances of
 * the <i>InstanceType</i> may be executed multiple times by a caller with different arguments. This
 * class must have an abstract method named {@code execute} which {@link ScriptEngine} implementations
 * will define.
 */
public final class ScriptContext<FactoryType> {

    /** A unique identifier for this context. */
    public final String name;

    /** A factory class for constructing instances of a script. */
    public final Class<FactoryType> factoryClazz;

    /** A class that is an instance of a script. */
    public final Class<?> instanceClazz;

    /** Construct a context with the related instance and compiled classes. */
    public ScriptContext(String name, Class<FactoryType> factoryClazz) {
        this.name = name;
        this.factoryClazz = factoryClazz;
        Method newInstanceMethod = null;
        for (Method method : factoryClazz.getMethods()) {
            if (method.getName().equals("newInstance")) {
                if (newInstanceMethod != null) {
                    throw new IllegalArgumentException("Cannot have multiple newInstance methods on FactoryType class ["
                        + factoryClazz.getName() + "] for script context [" + name + "]");
                }
                newInstanceMethod = method;
            }
        }
        if (newInstanceMethod == null) {
            throw new IllegalArgumentException("Could not find method newInstance on FactoryType class ["
                + factoryClazz.getName() + "] for script context [" + name + "]");
        }
        instanceClazz = newInstanceMethod.getReturnType();
    }
}
