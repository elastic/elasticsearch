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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Whitelist contains data structures designed to be used to generate a white-list of Java classes,
 * constructors, methods, and fields that can be used within a Painless script at both compile-time
 * and run-time.
 *
 * A white-list consists of several pieces with {@link Struct}s as the top level.  Each {@link Struct}
 * will contain zero-to-many {@link Constructor}s, {@link Method}s, and {@link Field}s which are what
 * will be available with a Painless script.  See each individual white-list object for more detail.
 */
public final class Whitelist {

    /**
     * Struct represents the equivalent of a Java class in Painless complete with super classes,
     * constructors, methods, and fields.  In Painless a class is known as a struct primarily to avoid
     * naming conflicts internally.  There must be a one-to-one mapping of struct names to Java classes.
     * Though, since multiple white-lists may be combined into a single white-list for a specific
     * {@link org.elasticsearch.script.ScriptContext}, as long as multiple structs representing the same
     * Java class have the same Painless type name and have legal constructor/method overloading they
     * can be merged together.
     *
     * Structs in Painless allow for arity overloading for constructors and methods.  Arity overloading
     * means that multiple constructors are allowed for a single struct as long as they have a different
     * number of parameter types, and multiples methods with the same name are allowed for a single struct
     * as long as they have the same return type and a different number of parameter types.
     *
     * Structs will automatically extend other white-listed structs if the Java class they represent is a
     * subclass of other structs including Java interfaces.
     */
    public static final class Struct {

        /** Information about where this struct was white-listed from.  Can be used for error messages. */
        public final String origin;

        /** The Painless name of this struct which will also be the name of a type in a Painless script.  */
        public final String painlessTypeName;

        /** The Java class name this struct represents. */
        public final String javaClassName;

        /** The {@link List} of white-listed ({@link Constructor}s) available to this struct. */
        public final List<Constructor> whitelistConstructors;

        /** The {@link List} of white-listed ({@link Method}s) available to this struct. */
        public final List<Method> whitelistMethods;

        /** The {@link List} of white-listed ({@link Field}s) available to this struct. */
        public final List<Field> whitelistFields;

        /** Standard constructor. All values must be not {@code null}. */
        public Struct(String origin, String painlessTypeName, String javaClassName,
                      List<Constructor> whitelistConstructors, List<Method> whitelistMethods, List<Field> whitelistFields) {
            this.origin = Objects.requireNonNull(origin);
            this.painlessTypeName = Objects.requireNonNull(painlessTypeName);
            this.javaClassName = Objects.requireNonNull(javaClassName);

            this.whitelistConstructors = Collections.unmodifiableList(Objects.requireNonNull(whitelistConstructors));
            this.whitelistMethods = Collections.unmodifiableList(Objects.requireNonNull(whitelistMethods));
            this.whitelistFields = Collections.unmodifiableList(Objects.requireNonNull(whitelistFields));
        }
    }

    /**
     * Constructor represents the equivalent of a Java constructor available as a white-listed struct
     * constructor within Painless.  Constructors for Painless structs may be accessed exactly as
     * constructors for Java classes are using the 'new' keyword.  Painless structs may have multiple
     * constructors as long as they comply with arity overloading described for {@link Struct}.
     */
    public static final class Constructor {

        /** Information about where this constructor was white-listed from.  Can be used for error messages. */
        public final String origin;

        /**
         * A {@link List} of {@link String}s that are the Painless type names for the parameters of the
         * constructor which can be used to look up the Java constructor through reflection.
         */
        public final List<String> painlessParameterTypeNames;

        /** Standard constructor. All values must be not {@code null}. */
        public Constructor(String origin, List<String> painlessParameterTypeNames) {
            this.origin = Objects.requireNonNull(origin);
            this.painlessParameterTypeNames = Collections.unmodifiableList(Objects.requireNonNull(painlessParameterTypeNames));
        }
    }

    /**
     * Method represents the equivalent of a Java method available as a white-listed struct method
     * within Painless.  Methods for Painless structs may be accessed exactly as methods for Java classes
     * are using the '.' operator on an existing struct variable/field.  Painless structs may have multiple
     * methods with the same name as long as they comply with arity overloading described for {@link Method}.
     *
     * Structs may also have additional methods that are not part of the Java class the struct represents -
     * these are known as augmented methods.  An augmented method can be added to a struct as a part of any
     * Java class as long as the method is static and the first parameter of the method is the Java class
     * represented by the struct.  Note that the augmented method's parent Java class does not need to be
     * white-listed.
     */
    public static class Method {

        /** Information about where this method was white-listed from.  Can be used for error messages. */
        public final String origin;

        /**
         * The Java class name for the owner of an augmented method.  If the method is not augmented
         * this should be {@code null}.
         */
        public final String javaAugmentedClassName;

        /** The Java method name used to look up the Java method through reflection. */
        public final String javaMethodName;

        /**
         * The Painless type name for the return type of the method which can be used to look up the Java
         * method through reflection.
         */
        public final String painlessReturnTypeName;

        /**
         * A {@link List} of {@link String}s that are the Painless type names for the parameters of the
         * method which can be used to look up the Java method through reflection.
         */
        public final List<String> painlessParameterTypeNames;

        /**
         * Standard constructor. All values must be not {@code null} with the exception of jAugmentedClass;
         * jAugmentedClass will be {@code null} unless the method is augmented as described in the class documentation.
         */
        public Method(String origin, String javaAugmentedClassName, String javaMethodName,
                      String painlessReturnTypeName, List<String> painlessParameterTypeNames) {
            this.origin = Objects.requireNonNull(origin);
            this.javaAugmentedClassName = javaAugmentedClassName;
            this.javaMethodName = javaMethodName;
            this.painlessReturnTypeName = Objects.requireNonNull(painlessReturnTypeName);
            this.painlessParameterTypeNames = Collections.unmodifiableList(Objects.requireNonNull(painlessParameterTypeNames));
        }
    }

    /**
     * Field represents the equivalent of a Java field available as a white-listed struct field
     * within Painless.  Fields for Painless structs may be accessed exactly as fields for Java classes
     * are using the '.' operator on an existing struct variable/field.
     */
    public static class Field {

        /** Information about where this method was white-listed from.  Can be used for error messages. */
        public final String origin;

        /** The Java field name used to look up the Java field through reflection. */
        public final String javaFieldName;

        /** The Painless type name for the field which can be used to look up the Java field through reflection. */
        public final String painlessFieldTypeName;

        /** Standard constructor.  All values must be not {@code null}. */
        public Field(String origin, String javaFieldName, String painlessFieldTypeName) {
            this.origin = Objects.requireNonNull(origin);
            this.javaFieldName = Objects.requireNonNull(javaFieldName);
            this.painlessFieldTypeName = Objects.requireNonNull(painlessFieldTypeName);
        }
    }

    /** The {@link ClassLoader} used to look up the white-listed Java classes, constructors, methods, and fields. */
    public final ClassLoader javaClassLoader;

    /** The {@link List} of all the white-listed Painless structs. */
    public final List<Struct> whitelistStructs;

    /** Standard constructor.  All values must be not {@code null}. */
    public Whitelist(ClassLoader javaClassLoader, List<Struct> whitelistStructs) {
        this.javaClassLoader = Objects.requireNonNull(javaClassLoader);
        this.whitelistStructs = Collections.unmodifiableList(Objects.requireNonNull(whitelistStructs));
    }
}
