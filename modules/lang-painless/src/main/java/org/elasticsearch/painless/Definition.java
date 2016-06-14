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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class Definition {

    private static final List<String> DEFINITION_FILES = Collections.unmodifiableList(
        Arrays.asList("org.elasticsearch.txt",
                      "java.lang.txt",
                      "java.math.txt",
                      "java.text.txt",
                      "java.time.txt",
                      "java.time.chrono.txt",
                      "java.time.format.txt",
                      "java.time.temporal.txt",
                      "java.time.zone.txt",
                      "java.util.txt",
                      "java.util.function.txt",
                      "java.util.regex.txt",
                      "java.util.stream.txt",
                      "joda.time.txt"));

    private static final Definition INSTANCE = new Definition();

    /** Some native types as constants: */
    public static final Type VOID_TYPE = getType("void");
    public static final Type BOOLEAN_TYPE = getType("boolean");
    public static final Type BOOLEAN_OBJ_TYPE = getType("Boolean");
    public static final Type BYTE_TYPE = getType("byte");
    public static final Type BYTE_OBJ_TYPE = getType("Byte");
    public static final Type SHORT_TYPE = getType("short");
    public static final Type SHORT_OBJ_TYPE = getType("Short");
    public static final Type INT_TYPE = getType("int");
    public static final Type INT_OBJ_TYPE = getType("Integer");
    public static final Type LONG_TYPE = getType("long");
    public static final Type LONG_OBJ_TYPE = getType("Long");
    public static final Type FLOAT_TYPE = getType("float");
    public static final Type FLOAT_OBJ_TYPE = getType("Float");
    public static final Type DOUBLE_TYPE = getType("double");
    public static final Type DOUBLE_OBJ_TYPE = getType("Double");
    public static final Type CHAR_TYPE = getType("char");
    public static final Type CHAR_OBJ_TYPE = getType("Character");
    public static final Type OBJECT_TYPE = getType("Object");
    public static final Type DEF_TYPE = getType("def");
    public static final Type STRING_TYPE = getType("String");
    public static final Type EXCEPTION_TYPE = getType("Exception");
    public static final Type PATTERN_TYPE = getType("Pattern");
    public static final Type MATCHER_TYPE = getType("Matcher");

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

        @Override
        public String toString() {
            return name;
        }
    }

    public static class Method {
        public final String name;
        public final Struct owner;
        public final Type rtn;
        public final List<Type> arguments;
        public final org.objectweb.asm.commons.Method method;
        public final int modifiers;
        public final MethodHandle handle;

        public Method(String name, Struct owner, Type rtn, List<Type> arguments,
                      org.objectweb.asm.commons.Method method, int modifiers, MethodHandle handle) {
            this.name = name;
            this.owner = owner;
            this.rtn = rtn;
            this.arguments = Collections.unmodifiableList(arguments);
            this.method = method;
            this.modifiers = modifiers;
            this.handle = handle;
        }
        
        /** 
         * Returns MethodType for this method.
         * <p>
         * This works even for user-defined Methods (where the MethodHandle is null).
         */
        public MethodType getMethodType() {
            // we have a methodhandle already (e.g. whitelisted class)
            // just return its type
            if (handle != null) {
                return handle.type();
            }
            // otherwise compute it
            final Class<?> params[];
            final Class<?> returnValue;
            if (Modifier.isStatic(modifiers)) {
                // static method: straightforward copy
                params = new Class<?>[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    params[i] = arguments.get(i).clazz;
                }
                returnValue = rtn.clazz;
            } else if ("<init>".equals(name)) {
                // constructor: returns the owner class
                params = new Class<?>[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    params[i] = arguments.get(i).clazz;
                }
                returnValue = owner.clazz;
            } else {
                // virtual/interface method: add receiver class
                params = new Class<?>[1 + arguments.size()];
                params[0] = owner.clazz;
                for (int i = 0; i < arguments.size(); i++) {
                    params[i + 1] = arguments.get(i).clazz;
                }
                returnValue = rtn.clazz;
            }
            return MethodType.methodType(returnValue, params);
        }
    }

    public static final class Field {
        public final String name;
        public final Struct owner;
        public final Type type;
        public final String javaName;
        public final int modifiers;
        private final MethodHandle getter;
        private final MethodHandle setter;

        private Field(String name, String javaName, Struct owner, Type type, int modifiers, MethodHandle getter, MethodHandle setter) {
            this.name = name;
            this.javaName = javaName;
            this.owner = owner;
            this.type = type;
            this.modifiers = modifiers;
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

        public final Map<MethodKey, Method> constructors;
        public final Map<MethodKey, Method> staticMethods;
        public final Map<MethodKey, Method> methods;

        public final Map<String, Field> staticMembers;
        public final Map<String, Field> members;

        private final SetOnce<Method> functionalMethod;

        private Struct(final String name, final Class<?> clazz, final org.objectweb.asm.Type type) {
            this.name = name;
            this.clazz = clazz;
            this.type = type;

            constructors = new HashMap<>();
            staticMethods = new HashMap<>();
            methods = new HashMap<>();

            staticMembers = new HashMap<>();
            members = new HashMap<>();

            functionalMethod = new SetOnce<>();
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

            functionalMethod = struct.functionalMethod;
        }

        private Struct freeze() {
            return new Struct(this);
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

        /**
         * If this class is a functional interface according to JLS, returns its method.
         * Otherwise returns null.
         */
        public Method getFunctionalMethod() {
            return functionalMethod.get();
        }
    }

    public static class Cast {
        public final Type from;
        public final Type to;
        public final boolean explicit;
        public final boolean unboxFrom;
        public final boolean unboxTo;
        public final boolean boxFrom;
        public final boolean boxTo;

        public Cast(final Type from, final Type to, final boolean explicit) {
            this.from = from;
            this.to = to;
            this.explicit = explicit;
            this.unboxFrom = false;
            this.unboxTo = false;
            this.boxFrom = false;
            this.boxTo = false;
        }

        public Cast(final Type from, final Type to, final boolean explicit,
                    final boolean unboxFrom, final boolean unboxTo, final boolean boxFrom, final boolean boxTo) {
            this.from = from;
            this.to = to;
            this.explicit = explicit;
            this.unboxFrom = unboxFrom;
            this.unboxTo = unboxTo;
            this.boxFrom = boxFrom;
            this.boxTo = boxTo;
        }

    }

    public static final class RuntimeClass {
        public final Map<MethodKey, Method> methods;
        public final Map<String, MethodHandle> getters;
        public final Map<String, MethodHandle> setters;

        private RuntimeClass(final Map<MethodKey, Method> methods,
                             final Map<String, MethodHandle> getters, final Map<String, MethodHandle> setters) {
            this.methods = Collections.unmodifiableMap(methods);
            this.getters = Collections.unmodifiableMap(getters);
            this.setters = Collections.unmodifiableMap(setters);
        }
    }

    /** Returns whether or not a non-array type exists. */
    public static boolean isSimpleType(final String name) {
        return INSTANCE.structsMap.containsKey(name);
    }

    /** Returns whether or not a type exists without an exception. */
    public static boolean isType(final String name) {
        try {
            INSTANCE.getTypeInternal(name);

            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    /** Gets the type given by its name */
    public static Type getType(final String name) {
        return INSTANCE.getTypeInternal(name);
    }

    /** Creates an array type from the given Struct. */
    public static Type getType(final Struct struct, final int dimensions) {
        return INSTANCE.getTypeInternal(struct, dimensions);
    }

    public static RuntimeClass getRuntimeClass(Class<?> clazz) {
        return INSTANCE.runtimeMap.get(clazz);
    }

    // INTERNAL IMPLEMENTATION:

    private final Map<Class<?>, RuntimeClass> runtimeMap;
    private final Map<String, Struct> structsMap;
    private final Map<String, Type> simpleTypesMap;

    private Definition() {
        structsMap = new HashMap<>();
        simpleTypesMap = new HashMap<>();
        runtimeMap = new HashMap<>();

        // parse the classes and return hierarchy (map of class name -> superclasses/interfaces)
        Map<String, List<String>> hierarchy = addStructs();
        // add every method for each class
        addElements();
        // apply hierarchy: this means e.g. copying Object's methods into String (thats how subclasses work)
        for (Map.Entry<String,List<String>> clazz : hierarchy.entrySet()) {
            copyStruct(clazz.getKey(), clazz.getValue());
        }
        // if someone declares an interface type, its still an Object
        for (Map.Entry<String,Struct> clazz : structsMap.entrySet()) {
            String name = clazz.getKey();
            Class<?> javaPeer = clazz.getValue().clazz;
            if (javaPeer.isInterface()) {
                copyStruct(name, Collections.singletonList("Object"));
            } else if (name.equals("def") == false && name.equals("Object") == false && javaPeer.isPrimitive() == false) {
                // but otherwise, unless its a primitive type, it really should
                assert hierarchy.get(name) != null : "class '" + name + "' does not extend Object!";
                assert hierarchy.get(name).contains("Object") : "class '" + name + "' does not extend Object!";
            }
        }
        // mark functional interfaces (or set null, to mark class is not)
        for (Struct clazz : structsMap.values()) {
            clazz.functionalMethod.set(computeFunctionalInterfaceMethod(clazz));
        }

        // precompute runtime classes
        for (Struct struct : structsMap.values()) {
          addRuntimeClass(struct);
        }
        // copy all structs to make them unmodifiable for outside users:
        for (final Map.Entry<String,Struct> entry : structsMap.entrySet()) {
            entry.setValue(entry.getValue().freeze());
        }
    }

    /** adds classes from definition. returns hierarchy */
    private Map<String,List<String>> addStructs() {
        final Map<String,List<String>> hierarchy = new HashMap<>();
        for (String file : DEFINITION_FILES) {
            int currentLine = -1;
            try {
                try (InputStream stream = Definition.class.getResourceAsStream(file);
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
                throw new RuntimeException("syntax error in " + file + ", line: " + currentLine, e);
            }
        }
        return hierarchy;
    }

    /** adds class methods/fields/ctors */
    private void addElements() {
        for (String file : DEFINITION_FILES) {
            int currentLine = -1;
            try {
                try (InputStream stream = Definition.class.getResourceAsStream(file);
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
                throw new RuntimeException("syntax error in " + file + ", line: " + currentLine, e);
            }
        }
    }

    private final void addStruct(final String name, final Class<?> clazz) {
        if (!name.matches("^[_a-zA-Z][\\.,_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid struct name [" + name + "].");
        }

        if (structsMap.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate struct name [" + name + "].");
        }

        final Struct struct = new Struct(name, clazz, org.objectweb.asm.Type.getType(clazz));

        structsMap.put(name, struct);
        simpleTypesMap.put(name, getTypeInternal(name));
    }

    private final void addConstructorInternal(final String struct, final String name, final Type[] args) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException(
                "Owner struct [" + struct + "] not defined for constructor [" + name + "].");
        }

        if (!name.matches("<init>")) {
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
        final Type returnType = getTypeInternal("void");
        final MethodHandle handle;

        try {
            handle = MethodHandles.publicLookup().in(owner.clazz).unreflectConstructor(reflect);
        } catch (final IllegalAccessException exception) {
            throw new IllegalArgumentException("Constructor " +
                " not found for class [" + owner.clazz.getName() + "]" +
                " with arguments " + Arrays.toString(classes) + ".");
        }

        final Method constructor = new Method(name, owner, returnType, Arrays.asList(args), asm, reflect.getModifiers(), handle);

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
        Type rtn = getTypeInternal(elements[0]);
        int parenIndex = elements[1].indexOf('(');
        if (parenIndex != -1) {
            // method or ctor
            int parenEnd = elements[1].indexOf(')');
            final Type args[];
            if (parenEnd > parenIndex + 1) {
                String arguments[] = elements[1].substring(parenIndex + 1, parenEnd).split(",");
                args = new Type[arguments.length];
                for (int i = 0; i < arguments.length; i++) {
                    args[i] = getTypeInternal(arguments[i]);
                }
            } else {
                args = new Type[0];
            }
            String methodName = elements[1].substring(0, parenIndex);
            if (methodName.equals("<init>")) {
                if (!elements[0].equals(className)) {
                    throw new IllegalArgumentException("Constructors must return their own type");
                }
                addConstructorInternal(className, "<init>", args);
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
                "Duplicate method signature [" + methodKey + "] found within the struct [" + owner.name + "].");
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

        final int modifiers = reflect.getModifiers();
        final Method method = new Method(name, owner, rtn, Arrays.asList(args), asm, modifiers, handle);

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

        final Field field = new Field(name, reflect.getName(), owner, type, modifiers, getter, setter);

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

    private void copyStruct(String struct, List<String> children) {
        final Struct owner = structsMap.get(struct);

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for copy.");
        }

        for (int count = 0; count < children.size(); ++count) {
            final Struct child = structsMap.get(children.get(count));

            if (child == null) {
                throw new IllegalArgumentException("Child struct [" + children.get(count) + "]" +
                    " not defined for copy to owner struct [" + owner.name + "].");
            }

            if (!child.clazz.isAssignableFrom(owner.clazz)) {
                throw new ClassCastException("Child struct [" + child.name + "]" +
                    " is not a super type of owner struct [" + owner.name + "] in copy.");
            }

            for (Map.Entry<MethodKey,Method> kvPair : child.methods.entrySet()) {
                MethodKey methodKey = kvPair.getKey();
                Method method = kvPair.getValue();
                if (owner.methods.get(methodKey) == null) {
                    // sanity check, look for missing covariant/generic override
                    if (owner.clazz.isInterface() && child.clazz == Object.class) {
                        // ok
                    } else if (child.clazz == Spliterator.OfPrimitive.class || child.clazz == PrimitiveIterator.class) {
                        // ok, we rely on generics erasure for these (its guaranteed in the javadocs though!!!!)
                    } else if (Constants.JRE_IS_MINIMUM_JAVA9 && owner.clazz == LocalDate.class) {
                        // ok, java 9 added covariant override for LocalDate.getEra() to return IsoEra:
                        // https://bugs.openjdk.java.net/browse/JDK-8072746
                    } else {
                        try {
                            Class<?> arguments[] = new Class<?>[method.arguments.size()];
                            for (int i = 0; i < method.arguments.size(); i++) {
                                arguments[i] = method.arguments.get(i).clazz;
                            }
                            java.lang.reflect.Method m = owner.clazz.getMethod(method.method.getName(), arguments);
                            if (m.getReturnType() != method.rtn.clazz) {
                                throw new IllegalStateException("missing covariant override for: " + m + " in " + owner.name);
                            }
                            if (m.isBridge() && !Modifier.isVolatile(method.modifiers)) {
                                // its a bridge in the destination, but not in the source, but it might still be ok, check generics:
                                java.lang.reflect.Method source = child.clazz.getMethod(method.method.getName(), arguments);
                                if (!Arrays.equals(source.getGenericParameterTypes(), source.getParameterTypes())) {
                                    throw new IllegalStateException("missing generic override for: " + m + " in " + owner.name);
                                }
                            }
                        } catch (ReflectiveOperationException e) {
                            throw new AssertionError(e);
                        }
                    }
                    owner.methods.put(methodKey, method);
                }
            }

            for (Field field : child.members.values()) {
                if (owner.members.get(field.name) == null) {
                    owner.members.put(field.name,
                        new Field(field.name, field.javaName, owner, field.type, field.modifiers, field.getter, field.setter));
                }
            }
        }
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

    /** computes the functional interface method for a class, or returns null */
    private Method computeFunctionalInterfaceMethod(Struct clazz) {
        if (!clazz.clazz.isInterface()) {
            return null;
        }
        // if its marked with this annotation, we fail if the conditions don't hold (means whitelist bug)
        // otherwise, this annotation is pretty useless.
        boolean hasAnnotation = clazz.clazz.isAnnotationPresent(FunctionalInterface.class);
        List<java.lang.reflect.Method> methods = new ArrayList<>();
        for (java.lang.reflect.Method m : clazz.clazz.getMethods()) {
            // default interface methods don't count
            if (m.isDefault()) {
                continue;
            }
            // static methods don't count
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            // if its from Object, it doesn't count
            try {
                Object.class.getMethod(m.getName(), m.getParameterTypes());
                continue;
            } catch (ReflectiveOperationException e) {
                // it counts
            }
            methods.add(m);
        }
        if (methods.size() != 1) {
            if (hasAnnotation) {
                throw new IllegalArgumentException("Class: " + clazz.name +
                                                   " is marked with FunctionalInterface but doesn't fit the bill: " + methods);
            }
            return null;
        }
        // inspect the one method found from the reflection API, it should match the whitelist!
        java.lang.reflect.Method oneMethod = methods.get(0);
        Method painless = clazz.methods.get(new Definition.MethodKey(oneMethod.getName(), oneMethod.getParameterCount()));
        if (painless == null || painless.method.equals(org.objectweb.asm.commons.Method.getMethod(oneMethod)) == false) {
            throw new IllegalArgumentException("Class: " + clazz.name + " is functional but the functional " +
                                               "method is not whitelisted!");
        }
        return painless;
    }

    private Type getTypeInternal(String name) {
        // simple types (e.g. 0 array dimensions) are a simple hash lookup for speed
        Type simple = simpleTypesMap.get(name);

        if (simple != null) {
            return simple;
        }

        int dimensions = getDimensions(name);
        String structstr = dimensions == 0 ? name : name.substring(0, name.indexOf('['));
        Struct struct = structsMap.get(structstr);

        if (struct == null) {
            throw new IllegalArgumentException("The struct with name [" + name + "] has not been defined.");
        }

        return getTypeInternal(struct, dimensions);
    }

    private Type getTypeInternal(Struct struct, int dimensions) {
        String name = struct.name;
        org.objectweb.asm.Type type = struct.type;
        Class<?> clazz = struct.clazz;
        Sort sort;

        if (dimensions > 0) {
            StringBuilder builder = new StringBuilder(name);
            char[] brackets = new char[dimensions];

            for (int count = 0; count < dimensions; ++count) {
                builder.append("[]");
                brackets[count] = '[';
            }

            String descriptor = new String(brackets) + struct.type.getDescriptor();

            name = builder.toString();
            type = org.objectweb.asm.Type.getType(descriptor);

            try {
                clazz = Class.forName(type.getInternalName().replace('/', '.'));
            } catch (ClassNotFoundException exception) {
                throw new IllegalArgumentException("The class [" + type.getInternalName() + "]" +
                    " could not be found to create type [" + name + "].");
            }

            sort = Sort.ARRAY;
        } else if ("def".equals(struct.name)) {
            sort = Sort.DEF;
        } else {
            sort = Sort.OBJECT;

            for (Sort value : Sort.values()) {
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

    private int getDimensions(String name) {
        int dimensions = 0;
        int index = name.indexOf('[');

        if (index != -1) {
            int length = name.length();

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
