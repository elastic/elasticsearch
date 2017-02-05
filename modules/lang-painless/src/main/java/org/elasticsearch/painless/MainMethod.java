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

package org.elasticsearch.painless;

import org.elasticsearch.painless.Definition.Type;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Information about the "main method" implemented by a Painless script.
 */
public class MainMethod {
    private final org.objectweb.asm.commons.Method asmMethod;
    private final List<MethodArgument> arguments;
    private final Map<String, DerivedArgument> derivedArguments;

    public MainMethod(Class<?> iface, DerivedArgument... derivedArguments) {
        if (false == iface.isAnnotationPresent(FunctionalInterface.class)) {
            throw new IllegalArgumentException("iface must be a @FunctionalInterface but was [" + iface.getName() + "]");
        }
        java.lang.reflect.Method mainMethod = null;
        for (java.lang.reflect.Method m : iface.getMethods()) {
            if (m.isDefault()) {
                continue;
            }
            if (mainMethod == null) {
                mainMethod = m;
            } else {
                throw new IllegalArgumentException("iface must have a single non-default method but has [" + mainMethod + "] and ["
                        + m + "]");
            }
        }
        if (mainMethod.isVarArgs()) {
            throw new IllegalArgumentException(
                    "Painless doesn't know how to compile varargs methods but tried to compile to [" + mainMethod + "]");
        }

        MethodType methodType = MethodType.methodType(mainMethod.getReturnType(), mainMethod.getParameterTypes());
        asmMethod = new org.objectweb.asm.commons.Method(mainMethod.getName(), methodType.toMethodDescriptorString());

        List<MethodArgument> arguments = new ArrayList<>();
        Annotation[][] annotations = mainMethod.getParameterAnnotations();
        Class<?>[] types = mainMethod.getParameterTypes();
        for (int arg = 0; arg < types.length; arg++) {
            Arg argInfo = null;
            for (Annotation ann : annotations[arg]) {
                if (ann.annotationType().equals(Arg.class)) {
                    argInfo = (Arg) ann;
                    break;
                }
            }
            if (argInfo == null) {
                throw new IllegalArgumentException("All arguments must be annotated with @Arg but the [" + (arg + 1) + "]th argument of ["
                        + mainMethod + "] was missing it.");
            }
            Definition.Type type;
            if (argInfo.type().equals("$infer$")) {
                Definition.RuntimeClass runtimeClass = Definition.getRuntimeClass(types[arg]);
                if (runtimeClass == null) {
                    throw new IllegalArgumentException("Can't infer Painless type for argument [" + argInfo.name()
                            + "]. Use the 'type' element of the @Arg annotation to specify a whitelisted type.");
                }
                type = Definition.getType(runtimeClass.getStruct(), 0);
            } else {
                try {
                    type = Definition.getType(argInfo.type());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "Argument type [" + argInfo.type() + "] on [" + argInfo.name() + "] isn't whitelisted.", e);
                }
                if (false == type.clazz.isAssignableFrom(types[arg]) && false == type.equals(Definition.DEF_TYPE)) {
                    throw new IllegalArgumentException("Painless argument type [" + type + "] not assignable from interface argument type ["
                            + org.objectweb.asm.Type.getType(types[arg]).getClassName() + "] for [" + argInfo.name() + "].");
                }
            }
            arguments.add(new MethodArgument(type, argInfo.name()));
        }
        this.arguments = unmodifiableList(arguments);

        Map<String, DerivedArgument> derivedArgumentsMap = new HashMap<>();
        for (DerivedArgument darg : derivedArguments) {
            Object old = derivedArgumentsMap.putIfAbsent(darg.getName(), darg);
            if (old != null) {
                throw new IllegalArgumentException("Duplicate derived argument name [" + darg.getName() + "]");
            }
        }
        this.derivedArguments = unmodifiableMap(derivedArgumentsMap);
    }

    public org.objectweb.asm.commons.Method getAsmMethod() {
        return asmMethod;
    }

    public List<MethodArgument> getArguments() {
        return arguments;
    }

    public Map<String, DerivedArgument> getDerivedArguments() {
        return derivedArguments;
    }

    public static class MethodArgument {
        private final Definition.Type type;
        private final String name;

        public MethodArgument(Definition.Type type, String name) {
            this.type = type;
            this.name = name;
        }

        public Definition.Type getType() {
            return type;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * A method argument derived from other method arguments. These are special because Painless won't declare them or set them unless they
     * are used.
     */
    public static class DerivedArgument {
        private final Definition.Type type;
        private final String name;
        private final BiConsumer<MethodWriter, Locals> deriver;

        public DerivedArgument(Type type, String name, BiConsumer<MethodWriter, Locals> deriver) {
            this.type = type;
            this.name = name;
            this.deriver = deriver;
        }

        public Definition.Type getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public BiConsumer<MethodWriter, Locals> getDeriver() {
            return deriver;
        }
    }
}
