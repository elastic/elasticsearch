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

import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.painless.WriterConstants.USES_PARAMETER_METHOD_TYPE;

/**
 * Information about the interface being implemented by the painless script.
 */
public class ScriptInterface {
    private final Class<?> iface;
    private final org.objectweb.asm.commons.Method executeMethod;
    private final Definition.Type executeMethodReturnType;
    private final List<MethodArgument> executeArguments;
    private final List<org.objectweb.asm.commons.Method> usesMethods;

    public ScriptInterface(Class<?> iface) {
        this.iface = iface;

        // Find the main method and the uses$argName methods
        java.lang.reflect.Method executeMethod = null;
        List<org.objectweb.asm.commons.Method> usesMethods = new ArrayList<>();
        for (java.lang.reflect.Method m : iface.getMethods()) {
            if (m.isDefault()) {
                continue;
            }
            if (m.getName().equals("execute")) {
                if (executeMethod == null) {
                    executeMethod = m;
                } else {
                    throw new IllegalArgumentException(
                            "Painless can only implement interfaces that have a single method named [execute] but [" + iface.getName()
                                    + "] has more than one.");
                }
                continue;
            }
            if (m.getName().startsWith("uses$")) {
                if (false == m.getReturnType().equals(boolean.class)) {
                    throw new IllegalArgumentException("Painless can only implement uses$ methods that return boolean but ["
                            + iface.getName() + "#" + m.getName() + "] returns [" + m.getReturnType().getName() + "].");
                }
                if (m.getParameterTypes().length > 0) {
                    throw new IllegalArgumentException("Painless can only implement uses$ methods that do not take parameters but ["
                            + iface.getName() + "#" + m.getName() + "] does.");
                }
                usesMethods.add(new org.objectweb.asm.commons.Method(m.getName(), USES_PARAMETER_METHOD_TYPE.toMethodDescriptorString()));
                continue;
            }
            throw new IllegalArgumentException("Painless can only implement methods named [execute] and [uses$argName] but ["
                    + iface.getName() + "] contains a method named [" + m.getName() + "]");
        }
        MethodType methodType = MethodType.methodType(executeMethod.getReturnType(), executeMethod.getParameterTypes());
        this.executeMethod = new org.objectweb.asm.commons.Method(executeMethod.getName(), methodType.toMethodDescriptorString());
        executeMethodReturnType = definitionTypeForClass(executeMethod.getReturnType(),
                componentType -> "Painless can only implement execute methods returning a whitelisted type but [" + iface.getName()
                        + "#execute] returns [" + componentType.getName() + "] which isn't whitelisted.");

        // Look up the argument names
        Set<String> argumentNames = new LinkedHashSet<>();
        List<MethodArgument> arguments = new ArrayList<>();
        String[] argumentNamesConstant = readArgumentNamesConstant(iface);
        Class<?>[] types = executeMethod.getParameterTypes();
        if (argumentNamesConstant.length != types.length) {
            throw new IllegalArgumentException("[" + iface.getName() + "#ARGUMENTS] has length [2] but ["
                    + iface.getName() + "#execute] takes [1] argument.");
        }
        for (int arg = 0; arg < types.length; arg++) {
            arguments.add(methodArgument(types[arg], argumentNamesConstant[arg]));
            argumentNames.add(argumentNamesConstant[arg]);
        }
        this.executeArguments = unmodifiableList(arguments);

        // Validate that the uses$argName methods reference argument names
        for (org.objectweb.asm.commons.Method usesMethod : usesMethods) {
            if (false == argumentNames.contains(usesMethod.getName().substring("uses$".length()))) {
                throw new IllegalArgumentException("Painless can only implement uses$ methods that match a parameter name but ["
                        + iface.getName() + "#" + usesMethod.getName() + "] doesn't match any of " + argumentNames + ".");
            }
        }
        this.usesMethods = unmodifiableList(usesMethods);
    }

    /**
     * The interface that the Painless script should implement.
     */
    public Class<?> getInterface() {
        return iface;
    }

    /**
     * An asm method descriptor for the {@code execute} method.
     */
    public org.objectweb.asm.commons.Method getExecuteMethod() {
        return executeMethod;
    }

    /**
     * The Painless {@link Definition.Type} or the return type of the {@code execute} method. This is used to generate the appropriate
     * return bytecode.
     */
    public Definition.Type getExecuteMethodReturnType() {
        return executeMethodReturnType;
    }

    /**
     * Painless {@link Definition.Type}s and names of the arguments to the {@code execute} method. The names are exposed to the Painless
     * script.
     */
    public List<MethodArgument> getExecuteArguments() {
        return executeArguments;
    }

    /**
     * The {@code uses$varName} methods that must be implemented by Painless to complete implementing the interface.
     */
    public List<org.objectweb.asm.commons.Method> getUsesMethods() {
        return usesMethods;
    }

    /**
     * Painless {@link Definition.Type}s and name of the argument to the {@code execute} method.
     */
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

    private static MethodArgument methodArgument(Class<?> type, String argName) {
        Definition.Type defType = definitionTypeForClass(type, componentType -> "[" + argName + "] is of unknown type ["
                + componentType.getName() + ". Painless interfaces can only accept arguments that are of whitelisted types.");
        return new MethodArgument(defType, argName);
    }

    private static Definition.Type definitionTypeForClass(Class<?> type, Function<Class<?>, String> unknownErrorMessageSource) {
        int dimensions = 0;
        Class<?> componentType = type;
        while (componentType.isArray()) {
            dimensions++;
            componentType = componentType.getComponentType();
        }
        Definition.Struct struct;
        if (componentType.equals(Object.class)) {
            struct = Definition.DEF_TYPE.struct;
        } else {
            Definition.RuntimeClass runtimeClass = Definition.getRuntimeClass(componentType);
            if (runtimeClass == null) {
                throw new IllegalArgumentException(unknownErrorMessageSource.apply(componentType));
            }
            struct = runtimeClass.getStruct();
        }
        return Definition.getType(struct, dimensions);
    }

    private static String[] readArgumentNamesConstant(Class<?> iface) {
        Field argumentNamesField;
        try {
            argumentNamesField = iface.getField("ARGUMENTS");
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Painless needs a constant [String[] ARGUMENTS] on all interfaces it implements with the "
                    + "names of the method arguments but [" + iface.getName() + "] doesn't have one.", e);
        }
        if (false == argumentNamesField.getType().equals(String[].class)) {
            throw new IllegalArgumentException("Painless needs a constant [String[] ARGUMENTS] on all interfaces it implements with the "
                    + "names of the method arguments but [" + iface.getName() + "] doesn't have one.");
        }
        try {
            return (String[]) argumentNamesField.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalArgumentException("Error trying to read [" + iface.getName() + "#ARGUMENTS]", e);
        }
    }
}
