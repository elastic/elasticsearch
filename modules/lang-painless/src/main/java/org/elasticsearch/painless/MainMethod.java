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

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Information about the "main method" implemented by a Painless script.
 */
public class MainMethod {
    private final org.objectweb.asm.commons.Method asmMethod;
    private final List<MethodArgument> arguments;

    public MainMethod(Class<?> iface) {
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
        if (mainMethod.getName().equals("getMetadata")) {
            throw new IllegalArgumentException("Painless cannot compile [" + iface.getName() + "] because it contains a method named "
                    + "[getMetadata] which can collide with PainlessScript#getMetadata");
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
            arguments.add(new MethodArgument(argType(argInfo.name(), types[arg]), argInfo.name()));
        }
        this.arguments = unmodifiableList(arguments);
    }

    public org.objectweb.asm.commons.Method getAsmMethod() {
        return asmMethod;
    }

    public List<MethodArgument> getArguments() {
        return arguments;
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

    private static Definition.Type argType(String argName, Class<?> type) {
        int dimensions = 0;
        while (type.isArray()) {
            dimensions++;
            type = type.getComponentType();
        }
        Definition.Struct struct;
        if (type.equals(Object.class)) {
            struct = Definition.DEF_TYPE.struct;
        } else {
            Definition.RuntimeClass runtimeClass = Definition.getRuntimeClass(type);
            if (runtimeClass == null) {
                throw new IllegalArgumentException("[" + argName + "] is of unknown type [" + type.getName()
                        + ". Painless interfaces can only accept arguments that are of whitelisted types.");
            }
            struct = runtimeClass.getStruct();
        }
        return Definition.getType(struct, dimensions);
    }
}
