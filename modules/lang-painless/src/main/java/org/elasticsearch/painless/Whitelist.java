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

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Whitelist contains data structures and methods designed to either load or generate a white-list of
 * Java classes, methods, and fields that can be used within a Painless script.  Whitelist is used directly
 * to create a new {@link Definition} and is passed into {@link Definition#Definition(Whitelist)}.
 * {@link Definition}'s are then used as part of each Painless script at both compile-time and
 * run-time to determine what a script is allowed to execute.
 *
 * Several single letters are used as prefixes for many variables in this class.  They mean the following:
 * p - Used as part of the Painless typing system
 * j - Used as part of the Java typing system
 * w = Used as part of the Whitelist typing system
 *
 * A Whitelist consists of two pieces.  The first is a list {@link Whitelist#pdynamics} of {@link String}s
 * that represent the allowed name(s) of dynamic types in Painless.  The second is a list of {@link WStruct}s
 * that represent the equivalent of a Java class in Painless known as a struct.
 */
public final class Whitelist {

    /**
     * WStruct represents the equivalent of a Java class in Painless complete with super classes,
     * constructors, methods, and fields.  In Painless a class is known as a struct primarily to avoid
     * naming conflicts internally.
     *
     * Structs in Painless allow for arity overloading for constructors and methods.  Arity overloading
     * means that multiple constructors are allowed for a single struct as long as they have a different
     * number of parameter types, and multiples methods with the same name are allowed for a single struct
     * as long as they have the same return type and a different number of parameter types.
     *
     * Structs are allowed to extend other structs, known as a super struct, in which they will have
     * available all of the methods and fields from a super struct.
     */
    public static class WStruct {
        /** The Painless name of this struct. */
        public final String pname;

        /** The Java class this struct represents. */
        public final Class<?> jclass;

        /** The {@link List} of Painless structs, as names, this struct extends. */
        public final List<String> psupers;

        /** The {@link List} of white-listed constructors {@link WConstructor} available to this struct. */
        public final List<WConstructor> wconstructors;

        /** The {@link List} of white-listed methods {@link WMethod} available to this struct. */
        public final List<WMethod> wmethods;

        /** The {@link List} of white-listed fields {@link WField} available to this struct. */
        public final List<WField> wfields;

        /** Standard constructor. All values must not be {@code null}. */
        public WStruct(String pname, Class<?> jclass, List<String> psupers,
                       List<WConstructor> wconstructors, List<WMethod> wmethods, List<WField> wfields) {
            this.pname = Objects.requireNonNull(pname);
            this.jclass = Objects.requireNonNull(jclass);
            this.psupers = Collections.unmodifiableList(Objects.requireNonNull(psupers));

            this.wconstructors = Collections.unmodifiableList(Objects.requireNonNull(wconstructors));
            this.wmethods = Collections.unmodifiableList(Objects.requireNonNull(wmethods));
            this.wfields = Collections.unmodifiableList(Objects.requireNonNull(wfields));
        }
    }

    public static class WConstructor {
        public final Constructor<?> constructor;
        public final List<String> parameterTypes;

        public WConstructor(Constructor<?> constructor, List<String> parameterTypes) {
            this.constructor = Objects.requireNonNull(constructor);
            this.parameterTypes = Collections.unmodifiableList(Objects.requireNonNull(parameterTypes));
        }
    }

    public static class WMethod {
        public final Method method;
        public final Class<?> augmented;
        public final String returnType;
        public final List<String> parameterTypes;

        public WMethod(Method method, Class<?> augmented, String returnType, List<String> parameterTypes) {
            this.method = Objects.requireNonNull(method);
            this.augmented = augmented;
            this.returnType = Objects.requireNonNull(returnType);
            this.parameterTypes = Collections.unmodifiableList(Objects.requireNonNull(parameterTypes));
        }
    }

    public static class WField {
        public final Field field;
        public final String type;

        public WField(Field field, String type) {
            this.field = Objects.requireNonNull(field);
            this.type = Objects.requireNonNull(type);
        }
    }

    public static Whitelist loadFromResourceFiles(ClassLoader loader, Map<Class<?>, List<String>> resourcesToFiles) {
        Map<String, Class<?>> namesToClasses = new HashMap<>();
        Set<String> pdynamics = new HashSet<>();

        for (Entry<Class<?>, List<String>> resourceToFiles : resourcesToFiles.entrySet()) {
            Class<?> resource = resourceToFiles.getKey();
            List<String> files = resourceToFiles.getValue();

            for (String file : files) {
                String line;
                int number = -1;

                try (LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(resource.getResourceAsStream(file), StandardCharsets.UTF_8))) {

                    while ((line = reader.readLine()) != null) {
                        number = reader.getLineNumber();
                        line = line.trim();

                        if (line.length() == 0 || line.charAt(0) == '#') {
                            continue;
                        }

                        if (line.startsWith("dynamic ")) {
                            String[] elements = line.split("\\s+");

                            if (elements.length != 2) {
                                throw new IllegalArgumentException("invalid dynamic definition: unexpected format [" + line + "]");
                            }

                            if (namesToClasses.containsKey(elements[1])) {
                                throw new IllegalArgumentException(
                                    "invalid dynamic definition: name [" + elements[1] + "] already defined as a struct");
                            }

                            pdynamics.add(elements[1]);
                        } else if (line.startsWith("class ")) {
                            if (line.endsWith("{") == false) {
                                throw new IllegalArgumentException(
                                    "invalid struct definition: failed to parse class opening bracket [" + line + "]");
                            }

                            int index = line.indexOf(" extends ");
                            String[] elements =
                                line.substring(5, index == -1 ? line.length() - 1 : index).replaceAll("\\s+", "").split("->");

                            if (elements.length < 1 || elements.length > 2) {
                                throw new IllegalArgumentException("invalid struct definition: failed to parse class name [" + line + "]");
                            }

                            String pname = elements[0];
                            String jname = elements.length == 2 ? elements[1] : pname;
                            Class<?> jclass;

                            if ("void".equals(jname)) {
                                jclass = void.class;
                            } else if ("boolean".equals(jname)) {
                                jclass = boolean.class;
                            } else if ("byte".equals(jname)) {
                                jclass = byte.class;
                            } else if ("short".equals(jname)) {
                                jclass = short.class;
                            } else if ("char".equals(jname)) {
                                jclass = char.class;
                            } else if ("int".equals(jname)) {
                                jclass = int.class;
                            } else if ("long".equals(jname)) {
                                jclass = long.class;
                            } else if ("float".equals(jname)) {
                                jclass = float.class;
                            } else if ("double".equals(jname)) {
                                jclass = double.class;
                            } else {
                                jclass = Class.forName(jname, true, loader);
                            }

                            if (pdynamics.contains(pname)) {
                                throw new IllegalArgumentException(
                                    "invalid struct definition: name [" + elements[1] + "] already defined as a dynamic");
                            }

                            Class<?> jduplicate = namesToClasses.get(pname);

                            if (jduplicate != null && jduplicate.equals(jclass) == false) {
                                throw new IllegalArgumentException("invalid struct definition: name [" + pname + "]" +
                                    " has been defined to be at least two different classes [" + jduplicate + "] and [" + jclass + "]");
                            }

                            namesToClasses.put(pname, jclass);
                        }
                    }
                } catch (Exception exception) {
                    throw new RuntimeException("error in [" + file + "] at line [" + number + "]", exception);
                }
            }
        }

        Function<String, Class<?>> getClassFromName = pname -> {
            int dimensions = 0;
            int index = pname.indexOf('[');
            String pnonarray = pname;

            if (index != -1) {
                int length = pname.length();

                while (index < length) {
                    if (pname.charAt(index) == '[' && ++index < length && pname.charAt(index++) == ']') {
                        ++dimensions;
                    } else {
                        throw new IllegalArgumentException("invalid struct/dynamic name [" + pname + "]");
                    }
                }

                pnonarray = pname.substring(0, pname.length() - dimensions*2);
            }

            Class<?> clazz = pdynamics.contains(pnonarray) ? Object.class : namesToClasses.get(pnonarray);

            if (clazz == null) {
                throw new IllegalArgumentException("invalid struct/dynamic name [" + pname + "]");
            }

            if (dimensions > 0) {
                pnonarray = clazz.getName();

                if ("boolean".equals(pnonarray)) {
                    pnonarray = "Z";
                } else if ("byte".equals(pnonarray)) {
                    pnonarray = "B";
                } else if ("short".equals(pnonarray)) {
                    pnonarray = "S";
                } else if ("char".equals(pnonarray)) {
                    pnonarray = "C";
                } else if ("int".equals(pnonarray)) {
                    pnonarray = "I";
                } else if ("long".equals(pnonarray)) {
                    pnonarray = "J";
                } else if ("float".equals(pnonarray)) {
                    pnonarray = "F";
                } else if ("double".equals(pnonarray)) {
                    pnonarray = "D";
                } else if ("void".equals(pnonarray)) {
                    pnonarray = "V";
                } else {
                    pnonarray = "L" + pnonarray + ";";
                }

                StringBuilder jarray = new StringBuilder();

                for (int dimension = 0; dimension < dimensions; ++dimension) {
                    jarray.append('[');
                }

                jarray.append(pnonarray);

                try {
                    clazz = Class.forName(jarray.toString(), true, loader);
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalArgumentException("invalid struct/dynamic name [" + pname + "]", cnfe);
                }
            }

            return clazz;
        };

        List<WStruct> wstructs = new ArrayList<>();

        for (Entry<Class<?>, List<String>> resourceToFiles : resourcesToFiles.entrySet()) {
            Class<?> resource = resourceToFiles.getKey();
            List<String> files = resourceToFiles.getValue();

            for (String file : files) {
                String line;
                int number = -1;

                try (LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(resource.getResourceAsStream(file), StandardCharsets.UTF_8))) {

                    String pname = null;
                    Class<?> jclass = null;
                    List<String> wsupers = null;
                    List<WConstructor> wconstructors = null;
                    List<WMethod> wmethods = null;
                    List<WField> wfields = null;

                    while ((line = reader.readLine()) != null) {
                        number = reader.getLineNumber();
                        line = line.trim();

                        if (line.length() == 0 || line.charAt(0) == '#' || line.startsWith("dynamic ")) {
                            continue;
                        }

                        if (line.startsWith("class ")) {
                            int index = line.indexOf(" extends ");
                            String[] elements =
                                line.substring(5, index == -1 ? line.length() - 1 : index).replaceAll("\\s+", "").split("->");

                            pname = elements[0];
                            jclass = namesToClasses.get(pname);
                            wsupers = Collections.emptyList();
                            wconstructors = new ArrayList<>();
                            wmethods = new ArrayList<>();
                            wfields = new ArrayList<>();

                            if (index != -1) {
                                elements = line.substring(index + 9, line.length() - 1).replaceAll("\\s+", "").split(",");
                                wsupers = Arrays.asList(elements);
                            }
                        } else if (line.equals("}")) {
                            if (pname == null) {
                                throw new IllegalArgumentException("invalid struct definition: extraneous closing bracket");
                            }

                            wstructs.add(new WStruct(pname, jclass, wsupers, wconstructors, wmethods, wfields));

                            pname = null;
                            jclass = null;
                            wsupers = null;
                            wconstructors = null;
                            wmethods = null;
                            wfields = null;
                        } else {
                            if (pname == null) {
                                throw new IllegalArgumentException("invalid object definition: expected a class name [" + line + "]");
                            }

                            if (line.startsWith("(")) {
                                if (line.endsWith(")") == false) {
                                    throw new IllegalArgumentException(
                                        "invalid constructor definition: expected a closing parenthesis [" + line + "]");
                                }

                                String[] elements = line.substring(1, line.length() - 1).replaceAll("\\s+", "").split(",");

                                if ("".equals(elements[0])) {
                                    elements = new String[0];
                                }

                                Class<?>[] jparameters = new Class<?>[elements.length];

                                for (int parameter = 0; parameter < jparameters.length; ++parameter) {
                                    try {
                                        jparameters[parameter] = getClassFromName.apply(elements[parameter]);
                                    } catch (IllegalArgumentException iae) {
                                        throw new IllegalArgumentException(
                                            "invalid constructor definition: invalid parameter [" + elements[parameter] + "]", iae);
                                    }
                                }

                                Constructor<?> jconstructor;

                                try {
                                    jconstructor = jclass.getConstructor(jparameters);
                                } catch (NoSuchMethodException nsme) {
                                    throw new IllegalArgumentException(
                                        "invalid constructor definition: failed to look up constructor [" + line + "]", nsme);
                                }

                                wconstructors.add(new WConstructor(jconstructor, Arrays.asList(elements)));
                            } else if (line.contains("(")) {
                                if (line.endsWith(")") == false) {
                                    throw new IllegalArgumentException(
                                        "invalid method definition: expected a closing parenthesis [" + line + "]");
                                }

                                int index = line.indexOf('(');
                                String[] elements = line.substring(0, index).split("\\s+");
                                String methodname;
                                Class<?> augmented;

                                if (elements.length == 2) {
                                    methodname = elements[1];
                                    augmented = null;
                                } else if (elements.length == 3) {
                                    methodname = elements[2];

                                    try {
                                        augmented = Class.forName(elements[1], true, loader);
                                    } catch (ClassNotFoundException cnfe) {
                                        throw new IllegalArgumentException(
                                            "invalid method definition: augmented class [" + elements[1] + "] does not exist", cnfe);
                                    }
                                } else {
                                    throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                                }

                                String preturn = elements[0];

                                try {
                                    getClassFromName.apply(preturn);
                                } catch (IllegalArgumentException iae) {
                                    throw new IllegalArgumentException("invalid method definition: invalid return [" + preturn + "]", iae);
                                }

                                elements = line.substring(index + 1, line.length() - 1).replaceAll("\\s+", "").split(",");
                                Class<?>[] jparameters;

                                if ("".equals(elements[0])) {
                                    elements = new String[0];
                                }

                                if (augmented == null) {
                                    jparameters = new Class<?>[elements.length];

                                    for (int parameter = 0; parameter < jparameters.length; ++parameter) {
                                        try {
                                            jparameters[parameter] = getClassFromName.apply(elements[parameter]);
                                        } catch (IllegalArgumentException iae) {
                                            throw new IllegalArgumentException(
                                                "invalid method definition: invalid parameter [" + elements[parameter] + "]", iae);
                                        }
                                    }
                                } else {
                                    jparameters = new Class<?>[1 + elements.length];
                                    jparameters[0] = jclass;

                                    for (int parameter = 1; parameter < jparameters.length; ++parameter) {
                                        try {
                                            jparameters[parameter] = getClassFromName.apply(elements[parameter - 1]);
                                        } catch (IllegalArgumentException iae) {
                                            throw new IllegalArgumentException(
                                                "invalid method definition: invalid parameter [" + elements[parameter - 1] + "]", iae);
                                        }
                                    }
                                }

                                Method jmethod;

                                try {
                                    jmethod = (augmented == null ? jclass : augmented).getMethod(methodname, jparameters);
                                } catch (NoSuchMethodException nsme) {
                                    throw new IllegalArgumentException(
                                        "invalid method definition: failed to look up method [" + line + "]", nsme);
                                }

                                wmethods.add(new WMethod(jmethod, augmented, preturn, Arrays.asList(elements)));
                            } else {
                                String[] elements = line.split("\\s+");

                                if (elements.length != 2) {
                                    throw new IllegalArgumentException("invalid field definition: unexpected format [" + line + "]");
                                }

                                try {
                                    getClassFromName.apply(elements[0]);
                                } catch (IllegalArgumentException iae) {
                                    throw new IllegalArgumentException("invalid field definition: invalid type [" + elements[0] + "]", iae);
                                }

                                Field jfield;

                                try {
                                    jfield = jclass.getField(elements[1]);
                                } catch (NoSuchFieldException nsfe) {
                                    throw new IllegalArgumentException(
                                        "invalid field definition: field [" + elements[0] + "] does not exist", nsfe);
                                }

                                wfields.add(new WField(jfield, elements[0]));
                            }
                        }
                    }

                    if (pname != null) {
                        throw new IllegalArgumentException("invalid struct definition: expected closing bracket");
                    }
                } catch (Exception exception) {
                    throw new RuntimeException("error in [" + file + "] at line [" + number + "]", exception);
                }
            }
        }

        return new Whitelist(pdynamics, wstructs);
    }

    public final Set<String> pdynamics;
    public final List<WStruct> wstructs;

    private Whitelist(Set<String> pdynamics, List<WStruct> wstructs) {
        this.pdynamics = Collections.unmodifiableSet(Objects.requireNonNull(pdynamics));
        this.wstructs = Collections.unmodifiableList(Objects.requireNonNull(wstructs));

        validate();
    }

    private void validate() {

    }
}
