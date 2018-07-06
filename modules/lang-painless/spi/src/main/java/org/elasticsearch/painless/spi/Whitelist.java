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

package org.elasticsearch.painless.spi;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Whitelist contains data structures designed to be used to generate a whitelist of Java classes,
 * constructors, methods, and fields that can be used within a Painless script at both compile-time
 * and run-time.
 *
 * A whitelist consists of several pieces with {@link WhitelistClass}s as the top level.  Each
 * {@link WhitelistClass} will contain zero-to-many {@link WhitelistConstructor}s, {@link Method}s, and
 * {@link Field}s which are what will be available with a Painless script.  See each individual
 * whitelist object for more detail.
 */
public final class Whitelist {

    private static final String[] BASE_WHITELIST_FILES = new String[] {
        "org.elasticsearch.txt",
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
        "joda.time.txt"
    };

    public static final List<Whitelist> BASE_WHITELISTS =
        Collections.singletonList(WhitelistLoader.loadFromResourceFiles(Whitelist.class, BASE_WHITELIST_FILES));

    /**
     * Method represents the equivalent of a Java method available as a whitelisted class method
     * within Painless.  Methods for Painless classes may be accessed exactly as methods for Java classes
     * are using the '.' operator on an existing class variable/field.  Painless classes may have multiple
     * methods with the same name as long as they comply with arity overloading described for {@link Method}.
     *
     * Classes may also have additional methods that are not part of the Java class the class represents -
     * these are known as augmented methods.  An augmented method can be added to a class as a part of any
     * Java class as long as the method is static and the first parameter of the method is the Java class
     * represented by the class.  Note that the augmented method's parent Java class does not need to be
     * whitelisted.
     */
    public static class Method {

        /** Information about where this method was whitelisted from.  Can be used for error messages. */
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
     * Field represents the equivalent of a Java field available as a whitelisted class field
     * within Painless.  Fields for Painless classes may be accessed exactly as fields for Java classes
     * are using the '.' operator on an existing class variable/field.
     */
    public static class Field {

        /** Information about where this method was whitelisted from.  Can be used for error messages. */
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

    /** The {@link ClassLoader} used to look up the whitelisted Java classes, constructors, methods, and fields. */
    public final ClassLoader javaClassLoader;

    /** The {@link List} of all the whitelisted Painless classes. */
    public final List<WhitelistClass> whitelistStructs;

    /** Standard constructor.  All values must be not {@code null}. */
    public Whitelist(ClassLoader javaClassLoader, List<WhitelistClass> whitelistStructs) {
        this.javaClassLoader = Objects.requireNonNull(javaClassLoader);
        this.whitelistStructs = Collections.unmodifiableList(Objects.requireNonNull(whitelistStructs));
    }
}
