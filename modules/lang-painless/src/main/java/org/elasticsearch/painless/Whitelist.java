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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

        /** The Painless name of this struct which will also be the name of a type in a Painless script.  */
        public final String painlessTypeName;

        /** The Java class this struct represents. */
        public final Class<?> javaClass;

        /** The {@link Set} of white-listed ({@link Constructor}s) available to this struct. */
        public final Set<Constructor> whitelistConstructors;

        /** The {@link Set} of white-listed ({@link Method}s) available to this struct. */
        public final Set<Method> whitelistMethods;

        /** The {@link Set} of white-listed ({@link Field}s) available to this struct. */
        public final Set<Field> whitelistFields;

        /** Standard constructor. All values must be not {@code null}. */
        public Struct(String painlessTypeName, Class<?> javaClass,
                      Set<Constructor> whitelistConstructors, Set<Method> whitelistMethods, Set<Field> whitelistFields) {
            this.painlessTypeName = Objects.requireNonNull(painlessTypeName);
            this.javaClass = Objects.requireNonNull(javaClass);

            this.whitelistConstructors = Collections.unmodifiableSet(Objects.requireNonNull(whitelistConstructors));
            this.whitelistMethods = Collections.unmodifiableSet(Objects.requireNonNull(whitelistMethods));
            this.whitelistFields = Collections.unmodifiableSet(Objects.requireNonNull(whitelistFields));
        }
    }

    /**
     * Constructor represents the equivalent of a Java constructor available as a white-listed struct
     * constructor within Painless.  Constructors for Painless structs may be accessed exactly as
     * constructors for Java classes are using the 'new' keyword.  Painless structs may have multiple
     * constructors as long as they comply with arity overloading described for {@link Struct}.
     */
    public static final class Constructor {

        /** The Java reflection {@link Constructor} available as a Painless struct constructor. */
        public final java.lang.reflect.Constructor<?> javaConstructor;

        /**
         * A {@link List} of {@link Boolean}s representing whether or not each of the constructor parameters,
         * in their respective ordering, is a Painless dynamic type (def), with {@code true} meaning the parameter
         * is a dynamic type and {@code false} meaning it's not.
         */
        public final List<Boolean> painlessDynamicParameters;

        /** Standard constructor. All values must be not {@code null}. */
        public Constructor(java.lang.reflect.Constructor<?> javaConstructor, List<Boolean> painlessDynamicParameters) {
            this.javaConstructor = Objects.requireNonNull(javaConstructor);
            this.painlessDynamicParameters = Collections.unmodifiableList(Objects.requireNonNull(painlessDynamicParameters));
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

        /** The Java reflection {@link Method} available as a Painless struct method. */
        public final java.lang.reflect.Method javaMethod;

        /** The Java {@link Class} owner for an augmented method.  If the method is not augmented this should be {@code null}. */
        public final Class<?> javaAugmentedClass;

        /**
         * Represents whether or not the return type is a Painless dynamic type (def), with {@code true} meaning the parameter
         * is a dynamic type and {@code false} meaning it's not.
         */
        public final boolean painlessDynamicReturn;

        /**
         * A {@link List} of {@link Boolean}s representing whether or not each of the method parameters,
         * in their respective ordering, is a Painless dynamic type (def), with {@code true} meaning the parameter
         * is a dynamic type and {@code false} meaning it's not.
         */
        public final List<Boolean> painlessDynamicParameters;

        /**
         * Standard constructor. All values must be not {@code null} with the exception of jAugmentedClass;
         * jAugmentedClass will be {@code null} unless the method is augmented as described in the class documentation.
         */
        public Method(java.lang.reflect.Method javaMethod, Class<?> javaAugmentedClass,
                       boolean painlessDynamicReturn, List<Boolean> painlessDynamicParameters) {
            this.javaMethod = Objects.requireNonNull(javaMethod);
            this.javaAugmentedClass = javaAugmentedClass;
            this.painlessDynamicReturn = painlessDynamicReturn;
            this.painlessDynamicParameters = Collections.unmodifiableList(Objects.requireNonNull(painlessDynamicParameters));
        }
    }

    /**
     * Field represents the equivalent of a Java field available as a white-listed struct field
     * within Painless.  Fields for Painless structs may be accessed exactly as fields for Java classes
     * are using the '.' operator on an existing struct variable/field.
     */
    public static class Field {

        /** The Java reflection {@link Field} available as a Painless struct field. */
        public final java.lang.reflect.Field javaField;

        /**
         * Represents whether or not the field type is a Painless dynamic type (def), with {@code true} meaning the parameter
         * is a dynamic type and {@code false} meaning it's not.
         */
        public final boolean painlessDynamic;

        /** Standard constructor. All values must be not {@code null}. */
        public  Field(java.lang.reflect.Field javaField, boolean painlessDynamic) {
            this.javaField = Objects.requireNonNull(javaField);
            this.painlessDynamic = painlessDynamic;
        }
    }

    /** The {@link Collection} of all the white-listed Painless structs. */
    public final Collection<Struct> whitelistStructs;

    /** Standard constructor.
     * @param whitelistStructs Cannot be {@code null}.
     */
    public Whitelist(Collection<Struct> whitelistStructs) {
        this.whitelistStructs = Collections.unmodifiableCollection(Objects.requireNonNull(whitelistStructs));
    }
}
