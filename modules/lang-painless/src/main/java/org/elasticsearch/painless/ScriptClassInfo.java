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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.painless.WriterConstants.NEEDS_PARAMETER_METHOD_TYPE;

/**
 * Information about the interface being implemented by the painless script.
 */
public class ScriptClassInfo {

    private final Class<?> baseClass;
    private final org.objectweb.asm.commons.Method executeMethod;
    private final Definition.Type executeMethodReturnType;
    private final List<MethodArgument> executeArguments;
    private final List<org.objectweb.asm.commons.Method> needsMethods;
    private final List<org.objectweb.asm.commons.Method> getMethods;
    private final List<Definition.Type> getReturns;

    public ScriptClassInfo(Definition definition, Class<?> baseClass) {
        this.baseClass = baseClass;

        // Find the main method and the uses$argName methods
        java.lang.reflect.Method executeMethod = null;
        List<org.objectweb.asm.commons.Method> needsMethods = new ArrayList<>();
        List<org.objectweb.asm.commons.Method> getMethods = new ArrayList<>();
        List<Definition.Type> getReturns = new ArrayList<>();
        for (java.lang.reflect.Method m : baseClass.getMethods()) {
            if (m.isDefault()) {
                continue;
            }
            if (m.getName().equals("execute")) {
                if (executeMethod == null) {
                    executeMethod = m;
                } else {
                    throw new IllegalArgumentException(
                            "Painless can only implement interfaces that have a single method named [execute] but [" + baseClass.getName()
                                    + "] has more than one.");
                }
            }
            if (m.getName().startsWith("needs") && m.getReturnType().equals(boolean.class) && m.getParameterTypes().length == 0) {
                needsMethods.add(new org.objectweb.asm.commons.Method(m.getName(), NEEDS_PARAMETER_METHOD_TYPE.toMethodDescriptorString()));
            }
            if (m.getName().startsWith("get") && m.getName().equals("getClass") == false && Modifier.isStatic(m.getModifiers()) == false) {
                getReturns.add(
                    definitionTypeForClass(definition, m.getReturnType(), componentType -> "[" + m.getName() + "] has unknown return type ["
                    + componentType.getName() + "]. Painless can only support getters with return types that are whitelisted."));

                getMethods.add(new org.objectweb.asm.commons.Method(m.getName(),
                    MethodType.methodType(m.getReturnType()).toMethodDescriptorString()));

            }
        }
        MethodType methodType = MethodType.methodType(executeMethod.getReturnType(), executeMethod.getParameterTypes());
        this.executeMethod = new org.objectweb.asm.commons.Method(executeMethod.getName(), methodType.toMethodDescriptorString());
        executeMethodReturnType = definitionTypeForClass(definition, executeMethod.getReturnType(),
                componentType -> "Painless can only implement execute methods returning a whitelisted type but [" + baseClass.getName()
                        + "#execute] returns [" + componentType.getName() + "] which isn't whitelisted.");

        // Look up the argument names
        Set<String> argumentNames = new LinkedHashSet<>();
        List<MethodArgument> arguments = new ArrayList<>();
        String[] argumentNamesConstant = readArgumentNamesConstant(baseClass);
        Class<?>[] types = executeMethod.getParameterTypes();
        if (argumentNamesConstant.length != types.length) {
            throw new IllegalArgumentException("[" + baseClass.getName() + "#ARGUMENTS] has length [2] but ["
                    + baseClass.getName() + "#execute] takes [1] argument.");
        }
        for (int arg = 0; arg < types.length; arg++) {
            arguments.add(methodArgument(definition, types[arg], argumentNamesConstant[arg]));
            argumentNames.add(argumentNamesConstant[arg]);
        }
        this.executeArguments = unmodifiableList(arguments);
        this.needsMethods = unmodifiableList(needsMethods);
        this.getMethods = unmodifiableList(getMethods);
        this.getReturns = unmodifiableList(getReturns);
    }

    /**
     * The interface that the Painless script should implement.
     */
    public Class<?> getBaseClass() {
        return baseClass;
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
    public List<org.objectweb.asm.commons.Method> getNeedsMethods() {
        return needsMethods;
    }

    /**
     * The {@code getVarName} methods that must be implemented by Painless to complete implementing the interface.
     */
    public List<org.objectweb.asm.commons.Method> getGetMethods() {
        return getMethods;
    }

    /**
     * The {@code getVarName} methods return types.
     */
    public List<Definition.Type> getGetReturns() {
        return getReturns;
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

    private MethodArgument methodArgument(Definition definition, Class<?> type, String argName) {
        Definition.Type defType = definitionTypeForClass(definition, type, componentType -> "[" + argName + "] is of unknown type ["
                + componentType.getName() + ". Painless interfaces can only accept arguments that are of whitelisted types.");
        return new MethodArgument(defType, argName);
    }

    private static Definition.Type definitionTypeForClass(Definition definition, Class<?> type,
            Function<Class<?>, String> unknownErrorMessageSource) {
        int dimensions = 0;
        Class<?> componentType = type;
        while (componentType.isArray()) {
            dimensions++;
            componentType = componentType.getComponentType();
        }
        Definition.Struct struct;
        if (componentType.equals(Object.class)) {
            struct = definition.DefType.struct;
        } else {
            Definition.RuntimeClass runtimeClass = definition.getRuntimeClass(componentType);
            if (runtimeClass == null) {
                throw new IllegalArgumentException(unknownErrorMessageSource.apply(componentType));
            }
            struct = runtimeClass.getStruct();
        }
        return definition.getType(struct, dimensions);
    }

    private static String[] readArgumentNamesConstant(Class<?> iface) {
        Field argumentNamesField;
        try {
            argumentNamesField = iface.getField("PARAMETERS");
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Painless needs a constant [String[] PARAMETERS] on all interfaces it implements with the "
                    + "names of the method arguments but [" + iface.getName() + "] doesn't have one.", e);
        }
        if (false == argumentNamesField.getType().equals(String[].class)) {
            throw new IllegalArgumentException("Painless needs a constant [String[] PARAMETERS] on all interfaces it implements with the "
                    + "names of the method arguments but [" + iface.getName() + "] doesn't have one.");
        }
        try {
            return (String[]) argumentNamesField.get(null);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalArgumentException("Error trying to read [" + iface.getName() + "#ARGUMENTS]", e);
        }
    }
}
