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
 * Method represents the equivalent of a Java method available as a whitelisted class method
 * within Painless.  Methods for Painless classes may be accessed exactly as methods for Java classes
 * are using the '.' operator on an existing class variable/field.  Painless classes may have multiple
 * methods with the same name as long as they comply with arity overloading described for {@link WhitelistMethod}.
 *
 * Classes may also have additional methods that are not part of the Java class the class represents -
 * these are known as augmented methods.  An augmented method can be added to a class as a part of any
 * Java class as long as the method is static and the first parameter of the method is the Java class
 * represented by the class.  Note that the augmented method's parent Java class does not need to be
 * whitelisted.
 */
public class WhitelistMethod {

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
    public WhitelistMethod(String origin, String javaAugmentedClassName, String javaMethodName,
                           String painlessReturnTypeName, List<String> painlessParameterTypeNames) {
        this.origin = Objects.requireNonNull(origin);
        this.javaAugmentedClassName = javaAugmentedClassName;
        this.javaMethodName = javaMethodName;
        this.painlessReturnTypeName = Objects.requireNonNull(painlessReturnTypeName);
        this.painlessParameterTypeNames = Collections.unmodifiableList(Objects.requireNonNull(painlessParameterTypeNames));
    }
}
