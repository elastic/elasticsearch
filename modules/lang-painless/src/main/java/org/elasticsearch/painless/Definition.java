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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class Definition {

    /**
     * The default language API to be used with Painless.  The second construction is used
     * to finalize all the variables, so there is no mistake of modification afterwards.
     */
    static Definition INSTANCE = new Definition(new Definition());

    public enum Sort {
        VOID(       void.class      , 0 , true  , false , false , false ),
        BOOL(       boolean.class   , 1 , true  , true  , false , true  ),
        BYTE(       byte.class      , 1 , true  , false , true  , true  ),
        SHORT(      short.class     , 1 , true  , false , true  , true  ),
        CHAR(       char.class      , 1 , true  , false , true  , true  ),
        INT(        int.class       , 1 , true  , false , true  , true  ),
        LONG(       long.class      , 2 , true  , false , true  , true  ),
        FLOAT(      float.class     , 1 , true  , false , true  , true  ),
        DOUBLE(     double.class    , 2 , true  , false , true  , true  ),

        VOID_OBJ(   Void.class      , 1 , true  , false , false , false ),
        BOOL_OBJ(   Boolean.class   , 1 , false , true  , false , false ),
        BYTE_OBJ(   Byte.class      , 1 , false , false , true  , false ),
        SHORT_OBJ(  Short.class     , 1 , false , false , true  , false ),
        CHAR_OBJ(   Character.class , 1 , false , false , true  , false ),
        INT_OBJ(    Integer.class   , 1 , false , false , true  , false ),
        LONG_OBJ(   Long.class      , 1 , false , false , true  , false ),
        FLOAT_OBJ(  Float.class     , 1 , false , false , true  , false ),
        DOUBLE_OBJ( Double.class    , 1 , false , false , true  , false ),

        NUMBER(     Number.class    , 1 , false , false , false , false ),
        STRING(     String.class    , 1 , false , false , false , true  ),

        OBJECT(     null            , 1 , false , false , false , false ),
        DEF(        null            , 1 , false , false , false , false ),
        ARRAY(      null            , 1 , false , false , false , false );

        public final Class<?> clazz;
        public final int size;
        public final boolean primitive;
        public final boolean bool;
        public final boolean numeric;
        public final boolean constant;

        Sort(final Class<?> clazz, final int size, final boolean primitive,
             final boolean bool, final boolean numeric, final boolean constant) {
            this.clazz = clazz;
            this.size = size;
            this.bool = bool;
            this.primitive = primitive;
            this.numeric = numeric;
            this.constant = constant;
        }
    }

    public static final class Type {
        public final String name;
        public final int dimensions;
        public final Struct struct;
        public final Class<?> clazz;
        public final org.objectweb.asm.Type type;
        public final Sort sort;

        private Type(final String name, final int dimensions, final Struct struct,
                     final Class<?> clazz, final org.objectweb.asm.Type type, final Sort sort) {
            this.name = name;
            this.dimensions = dimensions;
            this.struct = struct;
            this.clazz = clazz;
            this.type = type;
            this.sort = sort;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final Type type = (Type)object;

            return this.type.equals(type.type) && struct.equals(type.struct);
        }

        @Override
        public int hashCode() {
            int result = struct.hashCode();
            result = 31 * result + type.hashCode();

            return result;
        }
    }

    public static final class Constructor {
        public final String name;
        public final Struct owner;
        public final List<Type> arguments;
        public final org.objectweb.asm.commons.Method method;
        public final java.lang.reflect.Constructor<?> reflect;

        private Constructor(final String name, final Struct owner, final List<Type> arguments,
                            final org.objectweb.asm.commons.Method method, final java.lang.reflect.Constructor<?> reflect) {
            this.name = name;
            this.owner = owner;
            this.arguments = Collections.unmodifiableList(arguments);
            this.method = method;
            this.reflect = reflect;
        }
    }

    public static class Method {
        public final String name;
        public final Struct owner;
        public final Type rtn;
        public final List<Type> arguments;
        public final org.objectweb.asm.commons.Method method;
        public final java.lang.reflect.Method reflect;
        public final MethodHandle handle;

        private Method(final String name, final Struct owner, final Type rtn, final List<Type> arguments,
                       final org.objectweb.asm.commons.Method method, final java.lang.reflect.Method reflect,
                       final MethodHandle handle) {
            this.name = name;
            this.owner = owner;
            this.rtn = rtn;
            this.arguments = Collections.unmodifiableList(arguments);
            this.method = method;
            this.reflect = reflect;
            this.handle = handle;
        }
    }

    public static final class Field {
        public final String name;
        public final Struct owner;
        public final Type type;
        public final java.lang.reflect.Field reflect;
        public final MethodHandle getter;
        public final MethodHandle setter;

        private Field(final String name, final Struct owner, final Type type,
                      final java.lang.reflect.Field reflect, final MethodHandle getter, final MethodHandle setter) {
            this.name = name;
            this.owner = owner;
            this.type = type;
            this.reflect = reflect;
            this.getter = getter;
            this.setter = setter;
        }
    }

    // TODO: instead of hashing on this, we could have a 'next' pointer in Method itself, but it would make code more complex
    // please do *NOT* under any circumstances change this to be the crappy Tuple from elasticsearch!
    /**
     * Key for looking up a method.
     * <p>
     * Methods are keyed on both name and arity, and can be overloaded once per arity.
     * This allows signatures such as {@code String.indexOf(String) vs String.indexOf(String, int)}.
     * <p>
     * It is less flexible than full signature overloading where types can differ too, but
     * better than just the name, and overloading types adds complexity to users, too.
     */
    public static final class MethodKey {
        public final String name;
        public final int arity;

        /**
         * Create a new lookup key
         * @param name name of the method
         * @param arity number of parameters
         */
        public MethodKey(String name, int arity) {
            this.name = Objects.requireNonNull(name);
            this.arity = arity;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + arity;
            result = prime * result + name.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            MethodKey other = (MethodKey) obj;
            if (arity != other.arity) return false;
            if (!name.equals(other.name)) return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name);
            sb.append('/');
            sb.append(arity);
            return sb.toString();
        }
    }

    public static final class Struct {
        public final String name;
        public final Class<?> clazz;
        public final org.objectweb.asm.Type type;

        public final Map<MethodKey, Constructor> constructors;
        public final Map<MethodKey, Method> staticMethods;
        public final Map<MethodKey, Method> methods;

        public final Map<String, Field> staticMembers;
        public final Map<String, Field> members;

        private Struct(final String name, final Class<?> clazz, final org.objectweb.asm.Type type) {
            this.name = name;
            this.clazz = clazz;
            this.type = type;

            constructors = new HashMap<>();
            staticMethods = new HashMap<>();
            methods = new HashMap<>();

            staticMembers = new HashMap<>();
            members = new HashMap<>();
        }

        private Struct(final Struct struct) {
            name = struct.name;
            clazz = struct.clazz;
            type = struct.type;

            constructors = Collections.unmodifiableMap(struct.constructors);
            staticMethods = Collections.unmodifiableMap(struct.staticMethods);
            methods = Collections.unmodifiableMap(struct.methods);

            staticMembers = Collections.unmodifiableMap(struct.staticMembers);
            members = Collections.unmodifiableMap(struct.members);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            Struct struct = (Struct)object;

            return name.equals(struct.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    public static class Cast {
        public final Type from;
        public final Type to;
        public final boolean explicit;

        public Cast(final Type from, final Type to, final boolean explicit) {
            this.from = from;
            this.to = to;
            this.explicit = explicit;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final Cast cast = (Cast)object;

            return from.equals(cast.from) && to.equals(cast.to) && explicit == cast.explicit;
        }

        @Override
        public int hashCode() {
            int result = from.hashCode();
            result = 31 * result + to.hashCode();
            result = 31 * result + (explicit ? 1 : 0);

            return result;
        }
    }

    public static final class Transform extends Cast {
        public final Method method;
        public final Type upcast;
        public final Type downcast;

        public Transform(final Cast cast, Method method, final Type upcast, final Type downcast) {
            super(cast.from, cast.to, cast.explicit);

            this.method = method;
            this.upcast = upcast;
            this.downcast = downcast;
        }
    }

    public static final class RuntimeClass {
        public final Map<MethodKey, Method> methods;
        public final Map<String, MethodHandle> getters;
        public final Map<String, MethodHandle> setters;

        private RuntimeClass(final Map<MethodKey, Method> methods,
                             final Map<String, MethodHandle> getters, final Map<String, MethodHandle> setters) {
            this.methods = methods;
            this.getters = getters;
            this.setters = setters;
        }
    }

    final Map<Cast, Cast> transformsMap;
    final Map<Class<?>, RuntimeClass> runtimeMap;
    private final Map<String, Struct> structsMap;
    private final Map<String, Type> simpleTypesMap;

    private Definition() {
        structsMap = new HashMap<>();
        simpleTypesMap = new HashMap<>();
        transformsMap = new HashMap<>();
        runtimeMap = new HashMap<>();

        // parse the classes and return hierarchy (map of class name -> superclasses/interfaces)
        Map<String, List<String>> hierarchy = addStructs();
        // add every method for each class
        addElements();
        // apply hierarchy: this means e.g. copying Object's methods into String (thats how subclasses work)
        for (Map.Entry<String,List<String>> clazz : hierarchy.entrySet()) {
            copyStruct(clazz.getKey(), clazz.getValue());
        }
        addTransforms();
        // precompute runtime classes
        for (Struct struct : structsMap.values()) {
          addRuntimeClass(struct);
        }
    }

    private Definition(final Definition definition) {
        final Map<String, Struct> structs = new HashMap<>();

        for (final Struct struct : definition.structsMap.values()) {
            structs.put(struct.name, new Struct(struct));
        }

        this.structsMap = Collections.unmodifiableMap(structs);
        this.transformsMap = Collections.unmodifiableMap(definition.transformsMap);
        this.runtimeMap = Collections.unmodifiableMap(definition.runtimeMap);
        this.simpleTypesMap = Collections.unmodifiableMap(definition.simpleTypesMap);
    }

    /** adds classes from definition. returns hierarchy */
    private Map<String,List<String>> addStructs() {
        final Map<String,List<String>> hierarchy = new HashMap<>();
        int currentLine = -1;
        try {
            try (InputStream stream = Definition.class.getResourceAsStream("definition.txt");
                    LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    currentLine = reader.getLineNumber();
                    line = line.trim();
                    if (line.length() == 0 || line.charAt(0) == '#') {
                        continue;
                    }
                    if (line.startsWith("class ")) {
                        String elements[] = line.split("\u0020");
                        assert elements[2].equals("->");
                        if (elements.length == 7) {
                            hierarchy.put(elements[1], Arrays.asList(elements[5].split(",")));
                        } else {
                            assert elements.length == 5;
                        }
                        String className = elements[1];
                        String javaPeer = elements[3];
                        final Class<?> javaClazz;
                        switch (javaPeer) {
                            case "void":
                                javaClazz = void.class;
                                break;
                            case "boolean":
                                javaClazz = boolean.class;
                                break;
                            case "byte":
                                javaClazz = byte.class;
                                break;
                            case "short":
                                javaClazz = short.class;
                                break;
                            case "char":
                                javaClazz = char.class;
                                break;
                            case "int":
                                javaClazz = int.class;
                                break;
                            case "long":
                                javaClazz = long.class;
                                break;
                            case "float":
                                javaClazz = float.class;
                                break;
                            case "double":
                                javaClazz = double.class;
                                break;
                            default:
                                javaClazz = Class.forName(javaPeer);
                                break;
                        }
                        addStruct(className, javaClazz);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("syntax error in definition line: " + currentLine, e);
        }
        return hierarchy;
    }

    /** adds class methods/fields/ctors */
    private void addElements() {
        int currentLine = -1;
        try {
            try (InputStream stream = Definition.class.getResourceAsStream("definition.txt");
                 LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line = null;
                String currentClass = null;
                while ((line = reader.readLine()) != null) {
                    currentLine = reader.getLineNumber();
                    line = line.trim();
                    if (line.length() == 0 || line.charAt(0) == '#') {
                        continue;
                    } else if (line.startsWith("class ")) {
                        assert currentClass == null;
                        currentClass = line.split("\u0020")[1];
                    } else if (line.equals("}")) {
                        assert currentClass != null;
                        currentClass = null;
                    } else {
                        assert currentClass != null;
                        addSignature(currentClass, line);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("syntax error in definition line: " + currentLine, e);
        }
    }

    private void addTransforms() {
        Type booleanType = getType("boolean");
        Type objectType = getType("Object");
        Type defType = getType("def");
        Type booleanobjType = getType("Boolean");
        Type byteType = getType("byte");
        Type shortType = getType("short");
        Type intType = getType("int");
        Type charType = getType("char");
        Type longType = getType("long");
        Type floatType = getType("float");
        Type doubleType = getType("double");
        Type numberType = getType("Number");
        Type byteobjType = getType("Byte");
        Type shortobjType = getType("Short");
        Type charobjType = getType("Character");
        Type intobjType = getType("Integer");
        Type longobjType = getType("Long");
        Type floatobjType = getType("Float");
        Type doubleobjType = getType("Double");
        Type stringType = getType("String");
        
        addTransform(booleanType, objectType, "Boolean", "valueOf", true, false);
        addTransform(booleanType, defType, "Boolean", "valueOf", true, false);
        addTransform(booleanType, booleanobjType, "Boolean", "valueOf", true, false);

        addTransform(byteType, shortType, false);
        addTransform(byteType, charType, true);
        addTransform(byteType, intType, false);
        addTransform(byteType, longType, false);
        addTransform(byteType, floatType, false);
        addTransform(byteType, doubleType, false);
        addTransform(byteType, objectType, "Byte", "valueOf", true, false);
        addTransform(byteType, defType, "Byte", "valueOf", true, false);
        addTransform(byteType, numberType, "Byte", "valueOf", true, false);
        addTransform(byteType, byteobjType, "Byte", "valueOf", true, false);
        addTransform(byteType, shortobjType, "Utility", "byteToShort", true, false);
        addTransform(byteType, charobjType, "Utility", "byteToCharacter", true, true);
        addTransform(byteType, intobjType, "Utility", "byteToInteger", true, false);
        addTransform(byteType, longobjType, "Utility", "byteToLong", true, false);
        addTransform(byteType, floatobjType, "Utility", "byteToFloat", true, false);
        addTransform(byteType, doubleobjType, "Utility", "byteToDouble", true, false);

        addTransform(shortType, byteType, true);
        addTransform(shortType, charType, true);
        addTransform(shortType, intType, false);
        addTransform(shortType, longType, false);
        addTransform(shortType, floatType, false);
        addTransform(shortType, doubleType, false);
        addTransform(shortType, objectType, "Short", "valueOf", true, false);
        addTransform(shortType, defType, "Short", "valueOf", true, false);
        addTransform(shortType, numberType, "Short", "valueOf", true, false);
        addTransform(shortType, byteobjType, "Utility", "shortToByte", true, true);
        addTransform(shortType, shortobjType, "Short", "valueOf", true, false);
        addTransform(shortType, charobjType, "Utility", "shortToCharacter", true, true);
        addTransform(shortType, intobjType, "Utility", "shortToInteger", true, false);
        addTransform(shortType, longobjType, "Utility", "shortToLong", true, false);
        addTransform(shortType, floatobjType, "Utility", "shortToFloat", true, false);
        addTransform(shortType, doubleobjType, "Utility", "shortToDouble", true, false);

        addTransform(charType, byteType, true);
        addTransform(charType, shortType, true);
        addTransform(charType, intType, false);
        addTransform(charType, longType, false);
        addTransform(charType, floatType, false);
        addTransform(charType, doubleType, false);
        addTransform(charType, objectType, "Character", "valueOf", true, false);
        addTransform(charType, defType, "Character", "valueOf", true, false);
        addTransform(charType, numberType, "Utility", "charToInteger", true, false);
        addTransform(charType, byteobjType, "Utility", "charToByte", true, true);
        addTransform(charType, shortobjType, "Utility", "charToShort", true, true);
        addTransform(charType, charobjType, "Character", "valueOf", true, false);
        addTransform(charType, intobjType, "Utility", "charToInteger", true, false);
        addTransform(charType, longobjType, "Utility", "charToLong", true, false);
        addTransform(charType, floatobjType, "Utility", "charToFloat", true, false);
        addTransform(charType, doubleobjType, "Utility", "charToDouble", true, false);
        addTransform(charType, stringType, "Utility", "charToString", true, true);

        addTransform(intType, byteType, true);
        addTransform(intType, shortType, true);
        addTransform(intType, charType, true);
        addTransform(intType, longType, false);
        addTransform(intType, floatType, false);
        addTransform(intType, doubleType, false);
        addTransform(intType, objectType, "Integer", "valueOf", true, false);
        addTransform(intType, defType, "Integer", "valueOf", true, false);
        addTransform(intType, numberType, "Integer", "valueOf", true, false);
        addTransform(intType, byteobjType, "Utility", "intToByte", true, true);
        addTransform(intType, shortobjType, "Utility", "intToShort", true, true);
        addTransform(intType, charobjType, "Utility", "intToCharacter", true, true);
        addTransform(intType, intobjType, "Integer", "valueOf", true, false);
        addTransform(intType, longobjType, "Utility", "intToLong", true, false);
        addTransform(intType, floatobjType, "Utility", "intToFloat", true, false);
        addTransform(intType, doubleobjType, "Utility", "intToDouble", true, false);

        addTransform(longType, byteType, true);
        addTransform(longType, shortType, true);
        addTransform(longType, charType, true);
        addTransform(longType, intType, false);
        addTransform(longType, floatType, false);
        addTransform(longType, doubleType, false);
        addTransform(longType, objectType, "Long", "valueOf", true, false);
        addTransform(longType, defType, "Long", "valueOf", true, false);
        addTransform(longType, numberType, "Long", "valueOf", true, false);
        addTransform(longType, byteobjType, "Utility", "longToByte", true, true);
        addTransform(longType, shortobjType, "Utility", "longToShort", true, true);
        addTransform(longType, charobjType, "Utility", "longToCharacter", true, true);
        addTransform(longType, intobjType, "Utility", "longToInteger", true, true);
        addTransform(longType, longobjType, "Long", "valueOf", true, false);
        addTransform(longType, floatobjType, "Utility", "longToFloat", true, false);
        addTransform(longType, doubleobjType, "Utility", "longToDouble", true, false);

        addTransform(floatType, byteType, true);
        addTransform(floatType, shortType, true);
        addTransform(floatType, charType, true);
        addTransform(floatType, intType, true);
        addTransform(floatType, longType, false);
        addTransform(floatType, doubleType, false);
        addTransform(floatType, objectType, "Float", "valueOf", true, false);
        addTransform(floatType, defType, "Float", "valueOf", true, false);
        addTransform(floatType, numberType, "Float", "valueOf", true, false);
        addTransform(floatType, byteobjType, "Utility", "floatToByte", true, true);
        addTransform(floatType, shortobjType, "Utility", "floatToShort", true, true);
        addTransform(floatType, charobjType, "Utility", "floatToCharacter", true, true);
        addTransform(floatType, intobjType, "Utility", "floatToInteger", true, true);
        addTransform(floatType, longobjType, "Utility", "floatToLong", true, true);
        addTransform(floatType, floatobjType, "Float", "valueOf", true, false);
        addTransform(floatType, doubleobjType, "Utility", "floatToDouble", true, false);

        addTransform(doubleType, byteType, true);
        addTransform(doubleType, shortType, true);
        addTransform(doubleType, charType, true);
        addTransform(doubleType, intType, true);
        addTransform(doubleType, longType, true);
        addTransform(doubleType, floatType, false);
        addTransform(doubleType, objectType, "Double", "valueOf", true, false);
        addTransform(doubleType, defType, "Double", "valueOf", true, false);
        addTransform(doubleType, numberType, "Double", "valueOf", true, false);
        addTransform(doubleType, byteobjType, "Utility", "doubleToByte", true, true);
        addTransform(doubleType, shortobjType, "Utility", "doubleToShort", true, true);
        addTransform(doubleType, charobjType, "Utility", "doubleToCharacter", true, true);
        addTransform(doubleType, intobjType, "Utility", "doubleToInteger", true, true);
        addTransform(doubleType, longobjType, "Utility", "doubleToLong", true, true);
        addTransform(doubleType, floatobjType, "Utility", "doubleToFloat", true, true);
        addTransform(doubleType, doubleobjType, "Double", "valueOf", true, false);

        addTransform(objectType, booleanType, "Boolean", "booleanValue", false, true);
        addTransform(objectType, byteType, "Number", "byteValue", false, true);
        addTransform(objectType, shortType, "Number", "shortValue", false, true);
        addTransform(objectType, charType, "Character", "charValue", false, true);
        addTransform(objectType, intType, "Number", "intValue", false, true);
        addTransform(objectType, longType, "Number", "longValue", false, true);
        addTransform(objectType, floatType, "Number", "floatValue", false, true);
        addTransform(objectType, doubleType, "Number", "doubleValue", false, true);

        addTransform(defType, booleanType, "Boolean", "booleanValue", false, false);
        addTransform(defType, byteType, "Def", "DefTobyteImplicit", true, false);
        addTransform(defType, shortType, "Def", "DefToshortImplicit", true, false);
        addTransform(defType, charType, "Def", "DefTocharImplicit", true, false);
        addTransform(defType, intType, "Def", "DefTointImplicit", true, false);
        addTransform(defType, longType, "Def", "DefTolongImplicit", true, false);
        addTransform(defType, floatType, "Def", "DefTofloatImplicit", true, false);
        addTransform(defType, doubleType, "Def", "DefTodoubleImplicit", true, false);
        addTransform(defType, byteobjType, "Def", "DefToByteImplicit", true, false);
        addTransform(defType, shortobjType, "Def", "DefToShortImplicit", true, false);
        addTransform(defType, charobjType, "Def", "DefToCharacterImplicit", true, false);
        addTransform(defType, intobjType, "Def", "DefToIntegerImplicit", true, false);
        addTransform(defType, longobjType, "Def", "DefToLongImplicit", true, false);
        addTransform(defType, floatobjType, "Def", "DefToFloatImplicit", true, false);
        addTransform(defType, doubleobjType, "Def", "DefToDoubleImplicit", true, false);
        addTransform(defType, byteType, "Def", "DefTobyteExplicit", true, true);
        addTransform(defType, shortType, "Def", "DefToshortExplicit", true, true);
        addTransform(defType, charType, "Def", "DefTocharExplicit", true, true);
        addTransform(defType, intType, "Def", "DefTointExplicit", true, true);
        addTransform(defType, longType, "Def", "DefTolongExplicit", true, true);
        addTransform(defType, floatType, "Def", "DefTofloatExplicit", true, true);
        addTransform(defType, doubleType, "Def", "DefTodoubleExplicit", true, true);
        addTransform(defType, byteobjType, "Def", "DefToByteExplicit", true, true);
        addTransform(defType, shortobjType, "Def", "DefToShortExplicit", true, true);
        addTransform(defType, charobjType, "Def", "DefToCharacterExplicit", true, true);
        addTransform(defType, intobjType, "Def", "DefToIntegerExplicit", true, true);
        addTransform(defType, longobjType, "Def", "DefToLongExplicit", true, true);
        addTransform(defType, floatobjType, "Def", "DefToFloatExplicit", true, true);
        addTransform(defType, doubleobjType, "Def", "DefToDoubleExplicit", true, true);

        addTransform(numberType, byteType, "Number", "byteValue", false, true);
        addTransform(numberType, shortType, "Number", "shortValue", false, true);
        addTransform(numberType, charType, "Utility", "NumberTochar", true, true);
        addTransform(numberType, intType, "Number", "intValue", false, true);
        addTransform(numberType, longType, "Number", "longValue", false, true);
        addTransform(numberType, floatType, "Number", "floatValue", false, true);
        addTransform(numberType, doubleType, "Number", "doubleValue", false, true);
        addTransform(numberType, booleanobjType, "Utility", "NumberToBoolean", true, true);
        addTransform(numberType, byteobjType, "Utility", "NumberToByte", true, true);
        addTransform(numberType, shortobjType, "Utility", "NumberToShort", true, true);
        addTransform(numberType, charobjType, "Utility", "NumberToCharacter", true, true);
        addTransform(numberType, intobjType, "Utility", "NumberToInteger", true, true);
        addTransform(numberType, longobjType, "Utility", "NumberToLong", true, true);
        addTransform(numberType, floatobjType, "Utility", "NumberToFloat", true, true);
        addTransform(numberType, doubleobjType, "Utility", "NumberToDouble", true, true);

        addTransform(booleanobjType, booleanType, "Boolean", "booleanValue", false, false);

        addTransform(byteobjType, byteType, "Byte", "byteValue", false, false);
        addTransform(byteobjType, shortType, "Byte", "shortValue", false, false);
        addTransform(byteobjType, charType, "Utility", "ByteTochar", true, false);
        addTransform(byteobjType, intType, "Byte", "intValue", false, false);
        addTransform(byteobjType, longType, "Byte", "longValue", false, false);
        addTransform(byteobjType, floatType, "Byte", "floatValue", false, false);
        addTransform(byteobjType, doubleType, "Byte", "doubleValue", false, false);
        addTransform(byteobjType, shortobjType, "Utility", "NumberToShort", true, false);
        addTransform(byteobjType, charobjType, "Utility", "NumberToCharacter", true, false);
        addTransform(byteobjType, intobjType, "Utility", "NumberToInteger", true, false);
        addTransform(byteobjType, longobjType, "Utility", "NumberToLong", true, false);
        addTransform(byteobjType, floatobjType, "Utility", "NumberToFloat", true, false);
        addTransform(byteobjType, doubleobjType, "Utility", "NumberToDouble", true, false);

        addTransform(shortobjType, byteType, "Short", "byteValue", false, true);
        addTransform(shortobjType, shortType, "Short", "shortValue", false, true);
        addTransform(shortobjType, charType, "Utility", "ShortTochar", true, false);
        addTransform(shortobjType, intType, "Short", "intValue", false, false);
        addTransform(shortobjType, longType, "Short", "longValue", false, false);
        addTransform(shortobjType, floatType, "Short", "floatValue", false, false);
        addTransform(shortobjType, doubleType, "Short", "doubleValue", false, false);
        addTransform(shortobjType, byteobjType, "Utility", "NumberToByte", true, true);
        addTransform(shortobjType, charobjType, "Utility", "NumberToCharacter", true, true);
        addTransform(shortobjType, intobjType, "Utility", "NumberToInteger", true, false);
        addTransform(shortobjType, longobjType, "Utility", "NumberToLong", true, false);
        addTransform(shortobjType, floatobjType, "Utility", "NumberToFloat", true, false);
        addTransform(shortobjType, doubleobjType, "Utility", "NumberToDouble", true, false);

        addTransform(charobjType, byteType, "Utility", "CharacterTobyte", true, true);
        addTransform(charobjType, shortType, "Utility", "CharacterToshort", true, false);
        addTransform(charobjType, charType, "Character", "charValue", false, true);
        addTransform(charobjType, intType, "Utility", "CharacterToint", true, false);
        addTransform(charobjType, longType, "Utility", "CharacterTolong", true, false);
        addTransform(charobjType, floatType, "Utility", "CharacterTofloat", true, false);
        addTransform(charobjType, doubleType, "Utility", "CharacterTodouble", true, false);
        addTransform(charobjType, byteobjType, "Utility", "CharacterToByte", true, true);
        addTransform(charobjType, shortobjType, "Utility", "CharacterToShort", true, true);
        addTransform(charobjType, intobjType, "Utility", "CharacterToInteger", true, false);
        addTransform(charobjType, longobjType, "Utility", "CharacterToLong", true, false);
        addTransform(charobjType, floatobjType, "Utility", "CharacterToFloat", true, false);
        addTransform(charobjType, doubleobjType, "Utility", "CharacterToDouble", true, false);
        addTransform(charobjType, stringType, "Utility", "CharacterToString", true, true);

        addTransform(intobjType, byteType, "Integer", "byteValue", false, true);
        addTransform(intobjType, shortType, "Integer", "shortValue", false, true);
        addTransform(intobjType, charType, "Utility", "IntegerTochar", true, true);
        addTransform(intobjType, intType, "Integer", "intValue", false, false);
        addTransform(intobjType, longType, "Integer", "longValue", false, false);
        addTransform(intobjType, floatType, "Integer", "floatValue", false, false);
        addTransform(intobjType, doubleType, "Integer", "doubleValue", false, false);
        addTransform(intobjType, byteobjType, "Utility", "NumberToByte", true, true);
        addTransform(intobjType, shortobjType, "Utility", "NumberToShort", true, true);
        addTransform(intobjType, charobjType, "Utility", "NumberToCharacter", true, true);
        addTransform(intobjType, longobjType, "Utility", "NumberToLong", true, false);
        addTransform(intobjType, floatobjType, "Utility", "NumberToFloat", true, false);
        addTransform(intobjType, doubleobjType, "Utility", "NumberToDouble", true, false);

        addTransform(longobjType, byteType, "Long", "byteValue", false, true);
        addTransform(longobjType, shortType, "Long", "shortValue", false, true);
        addTransform(longobjType, charType, "Utility", "LongTochar", true, true);
        addTransform(longobjType, intType, "Long", "intValue", false, true);
        addTransform(longobjType, longType, "Long", "longValue", false, false);
        addTransform(longobjType, floatType, "Long", "floatValue", false, false);
        addTransform(longobjType, doubleType, "Long", "doubleValue", false, false);
        addTransform(longobjType, byteobjType, "Utility", "NumberToByte", true, true);
        addTransform(longobjType, shortobjType, "Utility", "NumberToShort", true, true);
        addTransform(longobjType, charobjType, "Utility", "NumberToCharacter", true, true);
        addTransform(longobjType, intobjType, "Utility", "NumberToInteger", true, true);
        addTransform(longobjType, floatobjType, "Utility", "NumberToFloat", true, false);
        addTransform(longobjType, doubleobjType, "Utility", "NumberToDouble", true, false);

        addTransform(floatobjType, byteType, "Float", "byteValue", false, true);
        addTransform(floatobjType, shortType, "Float", "shortValue", false, true);
        addTransform(floatobjType, charType, "Utility", "FloatTochar", true, true);
        addTransform(floatobjType, intType, "Float", "intValue", false, true);
        addTransform(floatobjType, longType, "Float", "longValue", false, true);
        addTransform(floatobjType, floatType, "Float", "floatValue", false, false);
        addTransform(floatobjType, doubleType, "Float", "doubleValue", false, false);
        addTransform(floatobjType, byteobjType, "Utility", "NumberToByte", true, true);
        addTransform(floatobjType, shortobjType, "Utility", "NumberToShort", true, true);
        addTransform(floatobjType, charobjType, "Utility", "NumberToCharacter", true, true);
        addTransform(floatobjType, intobjType, "Utility", "NumberToInteger", true, true);
        addTransform(floatobjType, longobjType, "Utility", "NumberToLong", true, true);
        addTransform(floatobjType, doubleobjType, "Utility", "NumberToDouble", true, false);

        addTransform(doubleobjType, byteType, "Double", "byteValue", false, true);
        addTransform(doubleobjType, shortType, "Double", "shortValue", false, true);
        addTransform(doubleobjType, charType, "Utility", "DoubleTochar", true, true);
        addTransform(doubleobjType, intType, "Double", "intValue", false, true);
        addTransform(doubleobjType, longType, "Double", "longValue", false, true);
        addTransform(doubleobjType, floatType, "Double", "floatValue", false, true);
        addTransform(doubleobjType, doubleType, "Double", "doubleValue", false, false);
        addTransform(doubleobjType, byteobjType, "Utility", "NumberToByte", true, true);
        addTransform(doubleobjType, shortobjType, "Utility", "NumberToShort", true, true);
        addTransform(doubleobjType, charobjType, "Utility", "NumberToCharacter", true, true);
        addTransform(doubleobjType, intobjType, "Utility", "NumberToInteger", true, true);
        addTransform(doubleobjType, longobjType, "Utility", "NumberToLong", true, true);
        addTransform(doubleobjType, floatobjType, "Utility", "NumberToFloat", true, true);

        addTransform(stringType, charType, "Utility", "StringTochar", true, true);
        addTransform(stringType, charobjType, "Utility", "StringToCharacter", true, true);
    }

    private final void addStruct(final String name, final Class<?> clazz) {
        if (!name.matches("^[_a-zA-Z][<>,_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid struct name [" + name + "].");
        }

        if (structsMap.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate struct name [" + name + "].");
        }

        final Struct struct = new Struct(name, clazz, org.objectweb.asm.Type.getType(clazz));

        structsMap.put(name, struct);
        simpleTypesMap.put(name, getType(name));
    }
    
    private final void addConstructorInternal(final String struct, final String name, final Type[] args) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException(
                "Owner struct [" + struct + "] not defined for constructor [" + name + "].");
        }

        if (!name.matches("^[_a-zA-Z][_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException(
                "Invalid constructor name [" + name + "] with the struct [" + owner.name + "].");
        }

        MethodKey methodKey = new MethodKey(name, args.length);

        if (owner.constructors.containsKey(methodKey)) {
            throw new IllegalArgumentException(
                "Duplicate constructor [" + methodKey + "] found within the struct [" + owner.name + "].");
        }

        if (owner.staticMethods.containsKey(methodKey)) {
            throw new IllegalArgumentException("Constructors and static methods may not have the same signature" +
                " [" + methodKey + "] within the same struct [" + owner.name + "].");
        }

        if (owner.methods.containsKey(methodKey)) {
            throw new IllegalArgumentException("Constructors and methods may not have the same signature" +
                " [" + methodKey + "] within the same struct [" + owner.name + "].");
        }

        final Class<?>[] classes = new Class<?>[args.length];

        for (int count = 0; count < classes.length; ++count) {
            classes[count] = args[count].clazz;
        }

        final java.lang.reflect.Constructor<?> reflect;

        try {
            reflect = owner.clazz.getConstructor(classes);
        } catch (final NoSuchMethodException exception) {
            throw new IllegalArgumentException("Constructor [" + name + "] not found for class" +
                " [" + owner.clazz.getName() + "] with arguments " + Arrays.toString(classes) + ".");
        }

        final org.objectweb.asm.commons.Method asm = org.objectweb.asm.commons.Method.getMethod(reflect);
        final Constructor constructor = new Constructor(name, owner, Arrays.asList(args), asm, reflect);

        owner.constructors.put(methodKey, constructor);
    }
    
    /** 
     * Adds a new signature to the definition.
     * <p>
     * Signatures have the following forms:
     * <ul>
     *   <li>{@code void method(String,int)}
     *   <li>{@code boolean field}
     *   <li>{@code Class <init>(String)}
     * </ul>
     * no spaces allowed.
     */
    private final void addSignature(String className, String signature) {
        String elements[] = signature.split("\u0020");
        if (elements.length != 2) {
            throw new IllegalArgumentException("Malformed signature: " + signature);
        }
        // method or field type (e.g. return type)
        Type rtn = getType(elements[0]);
        int parenIndex = elements[1].indexOf('(');
        if (parenIndex != -1) {
            // method or ctor
            int parenEnd = elements[1].indexOf(')');
            final Type args[];
            if (parenEnd > parenIndex + 1) {
                String arguments[] = elements[1].substring(parenIndex + 1, parenEnd).split(",");
                args = new Type[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    args[i] = getType(arguments[i]);
                }
            } else {
                args = new Type[0];
            }
            String methodName = elements[1].substring(0, parenIndex);
            if (methodName.equals("<init>")) {
                if (!elements[0].equals(className)) {
                    throw new IllegalArgumentException("Constructors must return their own type");
                }
                addConstructorInternal(className, "new", args);
            } else {
                if (methodName.indexOf('/') >= 0) {
                    String nameAndAlias[] = methodName.split("/");
                    if (nameAndAlias.length != 2) {
                        throw new IllegalArgumentException("Currently only two aliases are allowed!");
                    }
                    addMethodInternal(className, nameAndAlias[0], nameAndAlias[1], rtn, args);
                } else {
                    addMethodInternal(className, methodName, null, rtn, args);
                }
            }
        } else {
            // field
            addFieldInternal(className, elements[1], null, rtn);
        }
    }

    private final void addMethodInternal(final String struct, final String name, final String alias, 
                                         final Type rtn, final Type[] args) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined" +
                " for method [" + name + "].");
        }

        if (!name.matches("^[_a-zA-Z][_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid method name" +
                " [" + name + "] with the struct [" + owner.name + "].");
        }

        MethodKey methodKey = new MethodKey(name, args.length);

        if (owner.constructors.containsKey(methodKey)) {
            throw new IllegalArgumentException("Constructors and methods" +
                " may not have the same signature [" + methodKey + "] within the same struct" +
                " [" + owner.name + "].");
        }

        if (owner.staticMethods.containsKey(methodKey) || owner.methods.containsKey(methodKey)) {
            throw new IllegalArgumentException(
                "Duplicate  method signature [" + methodKey + "] found within the struct [" + owner.name + "].");
        }

        final Class<?>[] classes = new Class<?>[args.length];

        for (int count = 0; count < classes.length; ++count) {
            classes[count] = args[count].clazz;
        }

        final java.lang.reflect.Method reflect;

        try {
            reflect = owner.clazz.getMethod(alias == null ? name : alias, classes);
        } catch (final NoSuchMethodException exception) {
            throw new IllegalArgumentException("Method [" + (alias == null ? name : alias) + 
                "] not found for class [" + owner.clazz.getName() + "]" +
                " with arguments " + Arrays.toString(classes) + ".");
        }

        if (!reflect.getReturnType().equals(rtn.clazz)) {
            throw new IllegalArgumentException("Specified return type class [" + rtn.clazz + "]" +
                " does not match the found return type class [" + reflect.getReturnType() + "] for the" +
                " method [" + name + "]" +
                " within the struct [" + owner.name + "].");
        }

        final org.objectweb.asm.commons.Method asm = org.objectweb.asm.commons.Method.getMethod(reflect);

        MethodHandle handle;

        try {
            handle = MethodHandles.publicLookup().in(owner.clazz).unreflect(reflect);
        } catch (final IllegalAccessException exception) {
            throw new IllegalArgumentException("Method [" + (alias == null ? name : alias) + "]" +
                " not found for class [" + owner.clazz.getName() + "]" +
                " with arguments " + Arrays.toString(classes) + ".");
        }

        final Method method = new Method(name, owner, rtn, Arrays.asList(args), asm, reflect, handle);
        final int modifiers = reflect.getModifiers();

        if (java.lang.reflect.Modifier.isStatic(modifiers)) {
            owner.staticMethods.put(methodKey, method);
        } else {
            owner.methods.put(methodKey, method);
        }
    }
    
    private final void addFieldInternal(final String struct, final String name, final String alias,
                                        final Type type) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for " +
                " field [" + name + "].");
        }

        if (!name.matches("^[_a-zA-Z][_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid field " +
                " name [" + name + "] with the struct [" + owner.name + "].");
        }

        if (owner.staticMembers.containsKey(name) || owner.members.containsKey(name)) {
             throw new IllegalArgumentException("Duplicate field name [" + name + "]" +
                     " found within the struct [" + owner.name + "].");
        }

        java.lang.reflect.Field reflect;

        try {
            reflect = owner.clazz.getField(alias == null ? name : alias);
        } catch (final NoSuchFieldException exception) {
            throw new IllegalArgumentException("Field [" + (alias == null ? name : alias) + "]" +
                " not found for class [" + owner.clazz.getName() + "].");
        }
        
        final int modifiers = reflect.getModifiers();
        boolean isStatic = java.lang.reflect.Modifier.isStatic(modifiers);

        MethodHandle getter = null;
        MethodHandle setter = null;

        try {
            if (!isStatic) {
                getter = MethodHandles.publicLookup().unreflectGetter(reflect);
                setter = MethodHandles.publicLookup().unreflectSetter(reflect);
            }
        } catch (final IllegalAccessException exception) {
            throw new IllegalArgumentException("Getter/Setter [" + (alias == null ? name : alias) + "]" +
                " not found for class [" + owner.clazz.getName() + "].");
        }

        final Field field = new Field(name, owner, type, reflect, getter, setter);

        if (isStatic) {
            // require that all static fields are static final
            if (!java.lang.reflect.Modifier.isFinal(modifiers)) {
                throw new IllegalArgumentException("Static [" + name + "]" +
                    " within the struct [" + owner.name + "] is not final.");
            }

            owner.staticMembers.put(alias == null ? name : alias, field);
        } else {
            owner.members.put(alias == null ? name : alias, field);
        }
    }

    private final void copyStruct(final String struct, List<String> children) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for copy.");
        }

        for (int count = 0; count < children.size(); ++count) {
            final Struct child = structsMap.get(children.get(count));

            if (struct == null) {
                throw new IllegalArgumentException("Child struct [" + children.get(count) + "]" +
                    " not defined for copy to owner struct [" + owner.name + "].");
            }

            if (!child.clazz.isAssignableFrom(owner.clazz)) {
                throw new ClassCastException("Child struct [" + child.name + "]" +
                    " is not a super type of owner struct [" + owner.name + "] in copy.");
            }

            final boolean object = child.clazz.equals(Object.class) &&
                java.lang.reflect.Modifier.isInterface(owner.clazz.getModifiers());

            for (Map.Entry<MethodKey,Method> kvPair : child.methods.entrySet()) {
                MethodKey methodKey = kvPair.getKey();
                Method method = kvPair.getValue();
                if (owner.methods.get(methodKey) == null) {
                    final Class<?> clazz = object ? Object.class : owner.clazz;

                    java.lang.reflect.Method reflect;
                    MethodHandle handle;

                    try {
                        reflect = clazz.getMethod(method.method.getName(), method.reflect.getParameterTypes());
                    } catch (final NoSuchMethodException exception) {
                        throw new IllegalArgumentException("Method [" + method.method.getName() + "] not found for" +
                            " class [" + owner.clazz.getName() + "] with arguments " +
                            Arrays.toString(method.reflect.getParameterTypes()) + ".");
                    }

                    try {
                        handle = MethodHandles.publicLookup().in(owner.clazz).unreflect(reflect);
                    } catch (final IllegalAccessException exception) {
                        throw new IllegalArgumentException("Method [" + method.method.getName() + "] not found for" +
                            " class [" + owner.clazz.getName() + "] with arguments " +
                            Arrays.toString(method.reflect.getParameterTypes()) + ".");
                    }

                    owner.methods.put(methodKey,
                        new Method(method.name, owner, method.rtn, method.arguments, method.method, reflect, handle));
                }
            }

            for (final Field field : child.members.values()) {
                if (owner.members.get(field.name) == null) {
                    java.lang.reflect.Field reflect;
                    MethodHandle getter;
                    MethodHandle setter;

                    try {
                        reflect = owner.clazz.getField(field.reflect.getName());
                    } catch (final NoSuchFieldException exception) {
                        throw new IllegalArgumentException("Field [" + field.reflect.getName() + "]" +
                            " not found for class [" + owner.clazz.getName() + "].");
                    }

                    try {
                        getter = MethodHandles.publicLookup().unreflectGetter(reflect);
                        setter = MethodHandles.publicLookup().unreflectSetter(reflect);
                    } catch (final IllegalAccessException exception) {
                        throw new IllegalArgumentException("Getter/Setter [" + field.name + "]" +
                            " not found for class [" + owner.clazz.getName() + "].");
                    }

                    owner.members.put(field.name,
                        new Field(field.name, owner, field.type, reflect, getter, setter));
                }
            }
        }
    }

    private final void addTransform(final Type from, final Type to, final boolean explicit) {
        if (from.equals(to)) {
            throw new IllegalArgumentException("Transform cannot" +
                " have cast type from [" + from.name + "] be the same as cast type to [" + to.name + "].");
        }

        if (!from.sort.primitive || !to.sort.primitive) {
            throw new IllegalArgumentException("Only transforms between two primitives may be a simple cast, but" +
                "found [" + from.name + "] and [" + to.name + "].");
        }

        final Cast cast = new Cast(from, to, explicit);

        if (transformsMap.containsKey(cast)) {
            throw new IllegalArgumentException("Transform with " +
                " cast type from [" + from.name + "] to cast type to [" + to.name + "] already defined.");
        }

        transformsMap.put(cast, cast);
    }

    private final void addTransform(final Type from, final Type to, final String struct,
                                   final String name, final boolean statik, final boolean explicit) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for" +
                " transform with cast type from [" + from.name + "] and cast type to [" + to.name + "].");
        }

        if (from.equals(to)) {
            throw new IllegalArgumentException("Transform with owner struct [" + owner.name + "] cannot" +
                " have cast type from [" + from.name + "] be the same as cast type to [" + to.name + "].");
        }

        final Cast cast = new Cast(from, to, explicit);

        if (transformsMap.containsKey(cast)) {
            throw new IllegalArgumentException("Transform with owner struct [" + owner.name + "]" +
                " and cast type from [" + from.name + "] to cast type to [" + to.name + "] already defined.");
        }

        final Cast transform;

        final Method method;
        Type upcast = null;
        Type downcast = null;

        // transforms are implicitly arity of 0, unless a static method where its 1 (receiver passed)
        final MethodKey methodKey = new MethodKey(name, statik ? 1 : 0);

        if (statik) {
            method = owner.staticMethods.get(methodKey);

            if (method == null) {
                throw new IllegalArgumentException("Transform with owner struct [" + owner.name + "]" +
                    " and cast type from [" + from.name + "] to cast type to [" + to.name +
                    "] using a function [" + name + "] that is not defined.");
            }

            if (method.arguments.size() != 1) {
                throw new IllegalArgumentException("Transform with owner struct [" + owner.name + "]" +
                    " and cast type from [" + from.name + "] to cast type to [" + to.name +
                    "] using function [" + name + "] does not have a single type argument.");
            }

            Type argument = method.arguments.get(0);

            if (!argument.clazz.isAssignableFrom(from.clazz)) {
                if (from.clazz.isAssignableFrom(argument.clazz)) {
                    upcast = argument;
                } else {
                    throw new ClassCastException("Transform with owner struct [" + owner.name + "]" +
                        " and cast type from [" + from.name + "] to cast type to [" + to.name + "] using" +
                        " function [" + name + "] cannot cast from type to the function input argument type.");
                }
            }

            final Type rtn = method.rtn;

            if (!to.clazz.isAssignableFrom(rtn.clazz)) {
                if (rtn.clazz.isAssignableFrom(to.clazz)) {
                    downcast = to;
                } else {
                    throw new ClassCastException("Transform with owner struct [" + owner.name + "]" +
                        " and cast type from [" + from.name + "] to cast type to [" + to.name + "] using" +
                        " function [" + name + "] cannot cast to type to the function return argument type.");
                }
            }
        } else {
            method = owner.methods.get(methodKey);

            if (method == null) {
                throw new IllegalArgumentException("Transform with owner struct [" + owner.name + "]" +
                    " and cast type from [" + from.name + "] to cast type to [" + to.name +
                    "] using a method [" + name + "] that is not defined.");
            }

            if (!method.arguments.isEmpty()) {
                throw new IllegalArgumentException("Transform with owner struct [" + owner.name + "]" +
                    " and cast type from [" + from.name + "] to cast type to [" + to.name +
                    "] using method [" + name + "] does not have a single type argument.");
            }

            if (!owner.clazz.isAssignableFrom(from.clazz)) {
                if (from.clazz.isAssignableFrom(owner.clazz)) {
                    upcast = getType(owner.name);
                } else {
                    throw new ClassCastException("Transform with owner struct [" + owner.name + "]" +
                        " and cast type from [" + from.name + "] to cast type to [" + to.name + "] using" +
                        " method [" + name + "] cannot cast from type to the method input argument type.");
                }
            }

            final Type rtn = method.rtn;

            if (!to.clazz.isAssignableFrom(rtn.clazz)) {
                if (rtn.clazz.isAssignableFrom(to.clazz)) {
                    downcast = to;
                } else {
                    throw new ClassCastException("Transform with owner struct [" + owner.name + "]" +
                        " and cast type from [" + from.name + "] to cast type to [" + to.name + "]" +
                        " using method [" + name + "] cannot cast to type to the method return argument type.");
                }
            }
        }

        transform = new Transform(cast, method, upcast, downcast);
        transformsMap.put(cast, transform);
    }

    /**
     * Precomputes a more efficient structure for dynamic method/field access.
     */
    private void addRuntimeClass(final Struct struct) {
        final Map<MethodKey, Method> methods = struct.methods;
        final Map<String, MethodHandle> getters = new HashMap<>();
        final Map<String, MethodHandle> setters = new HashMap<>();

        // add all members
        for (final Map.Entry<String, Field> member : struct.members.entrySet()) {
            getters.put(member.getKey(), member.getValue().getter);
            setters.put(member.getKey(), member.getValue().setter);
        }

        // add all getters/setters
        for (final Map.Entry<MethodKey, Method> method : methods.entrySet()) {
            final String name = method.getKey().name;
            final Method m = method.getValue();

            if (m.arguments.size() == 0 &&
                name.startsWith("get") &&
                name.length() > 3 &&
                Character.isUpperCase(name.charAt(3))) {
                final StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(3)));
                newName.append(name.substring(4));
                getters.putIfAbsent(newName.toString(), m.handle);
            } else if (m.arguments.size() == 0 &&
                name.startsWith("is") &&
                name.length() > 2 &&
                Character.isUpperCase(name.charAt(2))) {
                final StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(2)));
                newName.append(name.substring(3));
                getters.putIfAbsent(newName.toString(), m.handle);
            }

            if (m.arguments.size() == 1 &&
                name.startsWith("set") &&
                name.length() > 3 &&
                Character.isUpperCase(name.charAt(3))) {
                final StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(3)));
                newName.append(name.substring(4));
                setters.putIfAbsent(newName.toString(), m.handle);
            }
        }

        runtimeMap.put(struct.clazz, new RuntimeClass(methods, getters, setters));
    }

    public final Type getType(final String name) {
        // simple types (e.g. 0 array dimensions) are a simple hash lookup for speed
        Type simple = simpleTypesMap.get(name);
        if (simple != null) {
            return simple;
        }
        final int dimensions = getDimensions(name);
        final String structstr = dimensions == 0 ? name : name.substring(0, name.indexOf('['));
        final Struct struct = structsMap.get(structstr);

        if (struct == null) {
            throw new IllegalArgumentException("The struct with name [" + name + "] has not been defined.");
        }

        return getType(struct, dimensions);
    }

    public final Type getType(final Struct struct, final int dimensions) {
        String name = struct.name;
        org.objectweb.asm.Type type = struct.type;
        Class<?> clazz = struct.clazz;
        Sort sort;

        if (dimensions > 0) {
            final StringBuilder builder = new StringBuilder(name);
            final char[] brackets = new char[dimensions];

            for (int count = 0; count < dimensions; ++count) {
                builder.append("[]");
                brackets[count] = '[';
            }

            final String descriptor = new String(brackets) + struct.type.getDescriptor();

            name = builder.toString();
            type = org.objectweb.asm.Type.getType(descriptor);

            try {
                clazz = Class.forName(type.getInternalName().replace('/', '.'));
            } catch (final ClassNotFoundException exception) {
                throw new IllegalArgumentException("The class [" + type.getInternalName() + "]" +
                    " could not be found to create type [" + name + "].");
            }

            sort = Sort.ARRAY;
        } else if ("def".equals(struct.name)) {
            sort = Sort.DEF;
        } else {
            sort = Sort.OBJECT;

            for (final Sort value : Sort.values()) {
                if (value.clazz == null) {
                    continue;
                }

                if (value.clazz.equals(struct.clazz)) {
                    sort = value;

                    break;
                }
            }
        }

        return new Type(name, dimensions, struct, clazz, type, sort);
    }

    private int getDimensions(final String name) {
        int dimensions = 0;
        int index = name.indexOf('[');

        if (index != -1) {
            final int length = name.length();

            while (index < length) {
                if (name.charAt(index) == '[' && ++index < length && name.charAt(index++) == ']') {
                    ++dimensions;
                } else {
                    throw new IllegalArgumentException("Invalid array braces in canonical name [" + name + "].");
                }
            }
        }

        return dimensions;
    }
}
