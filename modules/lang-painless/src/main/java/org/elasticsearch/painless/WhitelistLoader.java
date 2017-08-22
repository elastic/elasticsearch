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

import org.elasticsearch.painless.Whitelist.Constructor;
import org.elasticsearch.painless.Whitelist.Field;
import org.elasticsearch.painless.Whitelist.Method;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

public final class WhitelistLoader {

    /**
     * Loads and creates a {@link Whitelist} from one to many text files.  The file paths are passed in as an array of
     * {@link String}s with a single {@link Class} to be be used to load the resources where each {@link String}
     * is the path of a single text file.  The {@link Class}'s {@link ClassLoader} will be used to lookup the Java
     * reflection objects for each individual {@link Class}, {@link Constructor}, {@link Method}, and {@link Field}
     * specified as part of the white-list in the text file.
     *
     * Two passes are made through each specified text file.  The first pass will load the static Painless type names
     * that can then be used during a second pass as a lookup for Painless constructor/method/field return and parameter
     * type names.  The second pass will load each individual constructor, method, and field that will be white-listed
     * for use in Painless scripts.
     *
     * The following can be parsed from each white-list text file:
     * <ul>
     *   <li> Blank lines will be ignored by the parser. </li>
     *   <li> Comments may be created starting with a pound '#' symbol and end with a newline.  These will
     *   be ignored by the parser. </li>
     *   <li> Primitive types may be specified starting with 'class' and followed by the Painless type
     *   name (often the same as the Java type name), an arrow symbol, the Java type name,
     *   an opening bracket, a newline, a closing bracket, and a final newline. </li>
     *   <li> Complex types may be specified starting with 'class' and followed by the Painless type name,
     *   an arrow symbol, the Java class name, a opening bracket, a newline, constructor/method/field
     *   specifications, a closing bracket, and a final newline. Within a complex type the following
     *   may be parsed:
     *   <ul>
     *     <li> A constructor may be specified starting with an opening parenthesis, followed by a
     *     comma-delimited list of Painless type names corresponding to the type/class names for
     *     the equivalent Java parameter types (these must be white-listed as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li> A method may be specified starting with a Painless type name for the return type,
     *     followed by the Java name of the method (which will also be the Painless name for the
     *     method), an opening parenthesis, a comma-delimited list of Painless type names
     *     corresponding to the type/class names for the equivalent Java parameter types
     *     (these must be white-listed as well), a closing parenthesis, and a newline. </li>
     *     </li> An augmented method may be specified starting with a Painless type name for the return
     *     type, followed by the fully qualified Java name of the class the augmented method is
     *     part of (this class does not need to be white-listed), the Java name of the method
     *     (which will also be the Painless name for the method), an opening parenthesis, a
     *     comma-delimited list of Painless type names corresponding to the type/class names
     *     for the equivalent Java parameter types (these must be white-listed as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li>A field may be specified starting with a Painless type name for the equivalent Java type
     *     of the field, followed by the Java name of the field (which all be the Painless name
     *     for the field), and a newline. </li> </li>
     *   </ul>
     * </ul>
     *
     * Note there must be a one-to-one correspondence of Painless type names to Java type/class names.
     * If the same Painless type is defined across multiple files and the Java class is the same, all
     * specified constructors, methods, and fields will be merged into a single Painless type.  The
     * Painless dynamic type, 'def', used as part of constructor, method, and field definitions will
     * be appropriately parsed and handled.
     *
     * The following example is used to create a single white-list text file:
     *
     * {@code
     * # primitive types
     *
     * class int -> int {
     * }
     *
     * # complex types
     *
     * class Example -> my.package.Example {
     *   # constructors
     *   ()
     *   (int)
     *   (def, def)
     *   (Example, def)
     *
     *   # method
     *   Example add(int, def)
     *   int add(Example, Example)
     *   void example()
     *
     *   # augmented
     *   Example some.other.Class sub(Example, int, def)
     *
     *   # fields
     *   int value0
     *   int value1
     *   def value2
     * }
     * }
     */
    public static Whitelist loadFromResourceFiles(Class<?> resource, String... filepaths) {
        Map<String, Class<?>> painlessTypeNamesToJavaClasses = parseFirstPass(resource, filepaths);

        Map<String, WStruct> wStructs = new HashMap<>();
        String lambdaPDynamicTypeName = pDynamicTypeName;

        // Execute a second-pass through the white-list text files.  This will gather all the
        // constructors, methods, augmented methods, and fields for each white-listed struct.
        // Previously gathered data from the first-pass is used to ensure that all the type names
        // specified for both Painless and Java are valid.
        for (Entry<Class<?>, List<String>> resourceToFiles : resourcesToFiles.entrySet()) {
            Class<?> resource = resourceToFiles.getKey();
            List<String> files = resourceToFiles.getValue();

            // Function to retrieve a Java class from a Painless type name with support
            // for array-types using the ClassLoader from the current resource class to
            // ensure the Java class exists.
            Function<String, Class<?>> getJClassFromPTypeName = pTypeName -> {
                int arrayDimensions = 0;
                int firstBraceIndex = pTypeName.indexOf('[');
                String pNonArrayTypeName = pTypeName;

                // Count the number of dimensions for a possible array type, and set the appropriate non-array type.
                if (firstBraceIndex != -1) {
                    int length = pTypeName.length();

                    while (firstBraceIndex < length) {
                        if (pTypeName.charAt(firstBraceIndex) == '[' && ++firstBraceIndex < length &&
                            pTypeName.charAt(firstBraceIndex++) == ']') {
                            ++arrayDimensions;
                        } else {
                            throw new IllegalArgumentException("invalid struct/dynamic name [" + pTypeName + "]");
                        }
                    }

                    pNonArrayTypeName = pTypeName.substring(0, pTypeName.length() - arrayDimensions*2);
                }

                // Look up the non-array class using Object if the type is a dynamic.
                Class<?> jClass =
                    pNonArrayTypeName.equals(lambdaPDynamicTypeName) ? Object.class : painlessTypeNamesToJavaClasses.get(pNonArrayTypeName);

                // Ensure the class has been white-listed.
                if (jClass == null) {
                    throw new IllegalArgumentException("invalid struct/dynamic name [" + pTypeName + "]");
                }

                // If the Painless type name is an array type, look up the Java class for the array type.
                if (arrayDimensions > 0) {
                    String jNonArrayTypeName = jClass.getName();

                    if ("boolean".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "Z";
                    } else if ("byte".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "B";
                    } else if ("short".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "S";
                    } else if ("char".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "C";
                    } else if ("int".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "I";
                    } else if ("long".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "J";
                    } else if ("float".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "F";
                    } else if ("double".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "D";
                    } else if ("void".equals(jNonArrayTypeName)) {
                        jNonArrayTypeName = "V";
                    } else {
                        jNonArrayTypeName = "L" + jNonArrayTypeName + ";";
                    }

                    StringBuilder jArrayTypeName = new StringBuilder();

                    for (int dimension = 0; dimension < arrayDimensions; ++dimension) {
                        jArrayTypeName.append('[');
                    }

                    jArrayTypeName.append(jNonArrayTypeName);

                    try {
                        jClass = Class.forName(jArrayTypeName.toString(), true, resource.getClassLoader());
                    } catch (ClassNotFoundException cnfe) {
                        throw new IllegalArgumentException("invalid struct/dynamic name [" + pTypeName + "]", cnfe);
                    }
                }

                return jClass;
            };

            for (String file : files) {
                String line;
                int number = -1;

                try (LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(resource.getResourceAsStream(file), StandardCharsets.UTF_8))) {

                    String pTypeName = null;
                    Class<?> jClass = null;
                    List<String> pSuperTypeNames = null;
                    Set<WConstructor> wConstructors = null;
                    Set<WMethod> wMethods = null;
                    Set<WField> wFields = null;

                    while ((line = reader.readLine()) != null) {
                        number = reader.getLineNumber();
                        line = line.trim();

                        // Skip any lines that are either blank or comments.  Also skip any dynamic headers
                        // as there is no more useful information to gather from them in a second-pass.
                        if (line.length() == 0 || line.charAt(0) == '#' || line.startsWith("dynamic ")) {
                            continue;
                        }

                        // Handle a new Painless struct by resetting all the variables necessary to
                        // construct a new WStruct for the white-list.
                        // Expects the following format: 'class' ID -> ID ( 'extends' ID ( ',' ID )* )? '{' '\n'
                        if (line.startsWith("class ")) {
                            int extendsIndex = line.indexOf(" extends ");
                            String[] tokens =
                                line.substring(5, extendsIndex == -1 ? line.length() - 1 : extendsIndex).replaceAll("\\s+", "").split("->");

                            // Reset all the variables to support a new struct.
                            pTypeName = tokens[0];
                            jClass = painlessTypeNamesToJavaClasses.get(pTypeName);
                            pSuperTypeNames = Collections.emptyList();
                            wConstructors = new HashSet<>();
                            wMethods = new HashSet<>();
                            wFields = new HashSet<>();

                            // If the Painless type extends other Painless types, parse the list of type names and store them.
                            if (extendsIndex != -1) {
                                tokens = line.substring(extendsIndex + 9, line.length() - 1).replaceAll("\\s+", "").split(",");
                                pSuperTypeNames = Arrays.asList(tokens);
                            }

                            // Handle the end of a Painless struct, by creating a new WStruct with all the previously gathered
                            // constructors, methods, augmented methods, and fields, and adding it the list of white-listed structs.
                            // Expects the following format: '}' '\n'
                        } else if (line.equals("}")) {
                            if (pTypeName == null) {
                                throw new IllegalArgumentException("invalid struct definition: extraneous closing bracket");
                            }

                            WStruct wStruct = wStructs.get(pTypeName);

                            // If this Painless struct has already been defined, merge the new constructors, methods, augmented
                            // methods, and fields with the previous ones.
                            if (wStruct != null) {
                                // Ensure the super types are identical between the new struct and the old struct or merging
                                // cannot be completed.
                                if (pSuperTypeNames.equals(wStruct.pSuperTypeNames) == false) {
                                    throw new IllegalArgumentException("invalid struct definition: when attempting to merge " +
                                        "type [" + pTypeName + "] from multiple files, super types do not match");
                                }

                                wConstructors.addAll(wStruct.wConstructors);
                                wMethods.addAll(wStruct.wMethods);
                                wFields.addAll(wStruct.wFields);
                            }

                            wStructs.put(pTypeName, new WStruct(pTypeName, jClass, pSuperTypeNames, wConstructors, wMethods, wFields));

                            // Set all the variables to null to ensure a new struct definition is found before other parsable values.
                            pTypeName = null;
                            jClass = null;
                            pSuperTypeNames = null;
                            wConstructors = null;
                            wMethods = null;
                            wFields = null;

                            // Handle all other valid cases.
                        } else {
                            // Ensure we have a defined struct before adding any constructors, methods, augmented methods, or fields.
                            if (pTypeName == null) {
                                throw new IllegalArgumentException("invalid object definition: expected a class name [" + line + "]");
                            }

                            // Handle the case for a constructor definition.
                            // // Expects the following format: '(' ( ID ( ',' ID )* )? ')' '\n'
                            if (line.startsWith("(")) {
                                // Ensure the final token of the line is ')'.
                                if (line.endsWith(")") == false) {
                                    throw new IllegalArgumentException(
                                        "invalid constructor definition: expected a closing parenthesis [" + line + "]");
                                }

                                String[] tokens = line.substring(1, line.length() - 1).replaceAll("\\s+", "").split(",");

                                // Handle the case for a constructor with no parameters.
                                if ("".equals(tokens[0])) {
                                    tokens = new String[0];
                                }

                                Class<?>[] jParameterClasses = new Class<?>[tokens.length];

                                // Look up the Java class for each constructor parameter.
                                for (int parameterCount = 0; parameterCount < jParameterClasses.length; ++parameterCount) {
                                    try {
                                        jParameterClasses[parameterCount] = getJClassFromPTypeName.apply(tokens[parameterCount]);
                                    } catch (IllegalArgumentException iae) {
                                        throw new IllegalArgumentException(
                                            "invalid constructor definition: invalid parameter [" + tokens[parameterCount] + "]", iae);
                                    }
                                }

                                Constructor<?> jConstructor;

                                // Look up the Java constructor using the specified constructor parameters.
                                try {
                                    jConstructor = jClass.getConstructor(jParameterClasses);
                                } catch (NoSuchMethodException nsme) {
                                    throw new IllegalArgumentException(
                                        "invalid constructor definition: failed to look up constructor [" + line + "]", nsme);
                                }

                                wConstructors.add(new WConstructor(jConstructor, Arrays.asList(tokens)));

                                // Handle the case for a method or augmented method definition.
                                // Expects the following format: ID ID? ID '(' ( ID ( ',' ID )* )? ')' '\n'
                            } else if (line.contains("(")) {
                                // Ensure the final token of the line is ')'.
                                if (line.endsWith(")") == false) {
                                    throw new IllegalArgumentException(
                                        "invalid method definition: expected a closing parenthesis [" + line + "]");
                                }

                                // Parse the tokens prior to the method parameters.
                                int parameterIndex = line.indexOf('(');
                                String[] tokens = line.substring(0, parameterIndex).split("\\s+");

                                String jMethodName;
                                Class<?> jAugmentedClass;

                                // Based on the number of tokens, look up the Java method name and if provided the Java augmented class.
                                if (tokens.length == 2) {
                                    jMethodName = tokens[1];
                                    jAugmentedClass = null;
                                } else if (tokens.length == 3) {
                                    jMethodName = tokens[2];

                                    try {
                                        jAugmentedClass = Class.forName(tokens[1], true, resource.getClassLoader());
                                    } catch (ClassNotFoundException cnfe) {
                                        throw new IllegalArgumentException(
                                            "invalid method definition: augmented class [" + tokens[1] + "] does not exist", cnfe);
                                    }
                                } else {
                                    throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                                }

                                String pReturnTypeName = tokens[0];

                                // Look up the Java class for the method return type.
                                try {
                                    getJClassFromPTypeName.apply(pReturnTypeName);
                                } catch (IllegalArgumentException iae) {
                                    throw new IllegalArgumentException(
                                        "invalid method definition: invalid return [" + pReturnTypeName + "]", iae);
                                }

                                // Parse the method parameters.
                                tokens = line.substring(parameterIndex + 1, line.length() - 1).replaceAll("\\s+", "").split(",");
                                Class<?>[] jParameterClasses;

                                // Handle the case for a method with no parameters.
                                if ("".equals(tokens[0])) {
                                    tokens = new String[0];
                                }

                                // Look up the Java class for each method parameter.
                                if (jAugmentedClass == null) {
                                    jParameterClasses = new Class<?>[tokens.length];

                                    for (int parameterCount = 0; parameterCount < jParameterClasses.length; ++parameterCount) {
                                        try {
                                            jParameterClasses[parameterCount] = getJClassFromPTypeName.apply(tokens[parameterCount]);
                                        } catch (IllegalArgumentException iae) {
                                            throw new IllegalArgumentException(
                                                "invalid method definition: invalid parameter [" + tokens[parameterCount] + "]", iae);
                                        }
                                    }
                                } else {
                                    jParameterClasses = new Class<?>[1 + tokens.length];
                                    jParameterClasses[0] = jClass;

                                    for (int parameterCount = 1; parameterCount < jParameterClasses.length; ++parameterCount) {
                                        try {
                                            jParameterClasses[parameterCount] = getJClassFromPTypeName.apply(tokens[parameterCount - 1]);
                                        } catch (IllegalArgumentException iae) {
                                            throw new IllegalArgumentException(
                                                "invalid method definition: invalid parameter [" + tokens[parameterCount - 1] + "]", iae);
                                        }
                                    }
                                }

                                Method jmethod;

                                // Look up the Java method using the specified method return type, method name and method parameters.
                                try {
                                    jmethod =
                                        (jAugmentedClass == null ? jClass : jAugmentedClass).getMethod(jMethodName, jParameterClasses);
                                } catch (NoSuchMethodException nsme) {
                                    throw new IllegalArgumentException(
                                        "invalid method definition: failed to look up method [" + line + "]", nsme);
                                }

                                wMethods.add(new WMethod(jmethod, jAugmentedClass, pReturnTypeName, Arrays.asList(tokens)));
                                // Handle the case for a field definition.
                                // Expects the following format: ID ID '\n'
                            } else {
                                String[] tokens = line.split("\\s+");

                                // Ensure the correct number of tokens.
                                if (tokens.length != 2) {
                                    throw new IllegalArgumentException("invalid field definition: unexpected format [" + line + "]");
                                }

                                // Look up the Java class for the field.
                                try {
                                    getJClassFromPTypeName.apply(tokens[0]);
                                } catch (IllegalArgumentException iae) {
                                    throw new IllegalArgumentException("invalid field definition: invalid type [" + tokens[0] + "]", iae);
                                }

                                Field jField;

                                // Look up the Java field.
                                try {
                                    jField = jClass.getField(tokens[1]);
                                } catch (NoSuchFieldException nsfe) {
                                    throw new IllegalArgumentException(
                                        "invalid field definition: field [" + tokens[0] + "] does not exist", nsfe);
                                }

                                wFields.add(new WField(jField, tokens[0]));
                            }
                        }
                    }

                    // Ensure all structs end with a '}' token before the end of the file.
                    if (pTypeName != null) {
                        throw new IllegalArgumentException("invalid struct definition: expected closing bracket");
                    }
                } catch (Exception exception) {
                    throw new RuntimeException("error in [" + file + "] at line [" + number + "]", exception);
                }
            }
        }

        return new Whitelist(pDynamicTypeName, wStructs.values());
    }

    // Execute a first-pass through the white-list text files.  This will gather
    // and create a map of static Painless type names to their corresponding Java
    // types/classes for later use during lookup of return and parameter types
    // for constructors, fields, and methods.
    private static Map<String, Class<?>> parseFirstPass(Class<?> resource, String... filepaths) {
        Map<String, Class<?>> painlessTypeNamesToJavaClasses = new HashMap<>();

        for (String filepath : filepaths) {
            String line;
            int number = -1;

            try (LineNumberReader reader = new LineNumberReader(
                new InputStreamReader(resource.getResourceAsStream(filepath), StandardCharsets.UTF_8))) {

                while ((line = reader.readLine()) != null) {
                    number = reader.getLineNumber();
                    line = line.trim();

                    // Skip any lines that are either blank or comments.
                    if (line.length() == 0 || line.charAt(0) == '#') {
                        continue;
                    }

                    // Handle the case where Painless primitive/complex type names are listed.
                    // Expects the following format: 'class' ID -> ID ( 'extends' ID ( ',' ID )* )? '{' '\n'
                    if (line.startsWith("class ")) {
                        // Ensure the final token of the line is '{'.
                        if (line.endsWith("{") == false) {
                            throw new IllegalArgumentException(
                                "invalid struct definition: failed to parse class opening bracket [" + line + "]");
                        }

                        // Parse based on the expected format prior to the 'extends' token.
                        String[] tokens = line.substring(5, line.length() - 1).replaceAll("\\s+", "").split("->");

                        // Ensure the correct number of tokens.
                        if (tokens.length != 2) {
                            throw new IllegalArgumentException("invalid struct definition: failed to parse class name [" + line + "]");
                        }

                        String painlessTypeName = tokens[0];

                        if (painlessTypeName.equals(Definition.DYNAMIC_TYPE_NAME)) {
                            throw new IllegalArgumentException(
                                "invalid struct definition: [" + painlessTypeName + "] already defined as a dynamic type name");
                        }

                        String javaClassName = tokens[1];
                        Class<?> javaClass;

                        // Retrieve the Java class from the Java class name using the ClassLoader from
                        // the current resource class to ensure the Java class exists.
                        if ("void".equals(javaClassName)) {
                            javaClass = void.class;
                        } else if ("boolean".equals(javaClassName)) {
                            javaClass = boolean.class;
                        } else if ("byte".equals(javaClassName)) {
                            javaClass = byte.class;
                        } else if ("short".equals(javaClassName)) {
                            javaClass = short.class;
                        } else if ("char".equals(javaClassName)) {
                            javaClass = char.class;
                        } else if ("int".equals(javaClassName)) {
                            javaClass = int.class;
                        } else if ("long".equals(javaClassName)) {
                            javaClass = long.class;
                        } else if ("float".equals(javaClassName)) {
                            javaClass = float.class;
                        } else if ("double".equals(javaClassName)) {
                            javaClass = double.class;
                        } else {
                            javaClass = Class.forName(javaClassName, true, resource.getClassLoader());
                        }

                        Class<?> jDuplicateClass = painlessTypeNamesToJavaClasses.get(painlessTypeName);

                        // Ensure that if a Java class has already been associated with a Painless type name,
                        // the same association is made if there are duplicate definitions.
                        if (jDuplicateClass != null && jDuplicateClass.equals(javaClass) == false) {
                            throw new IllegalArgumentException("invalid struct definition: name [" + painlessTypeName + "] has been " +
                                "defined to be at least two different classes [" + jDuplicateClass + "] and [" + javaClass + "]");
                        }

                        painlessTypeNamesToJavaClasses.put(painlessTypeName, javaClass);
                    }
                }
            } catch (Exception exception) {
                throw new RuntimeException("error in [" + filepath + "] at line [" + number + "]", exception);
            }
        }

        return painlessTypeNamesToJavaClasses;
    }

    private WhitelistLoader() {

    }
}
