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
import java.util.List;

/** Loads and creates a {@link Whitelist} from one to many text files. */
public final class WhitelistLoader {

    /**
     * Loads and creates a {@link Whitelist} from one to many text files.  The file paths are passed in as an array of
     * {@link String}s with a single {@link Class} to be be used to load the resources where each {@link String}
     * is the path of a single text file.  The {@link Class}'s {@link ClassLoader} will be used to lookup the Java
     * reflection objects for each individual {@link Class}, {@link Constructor}, {@link Method}, and {@link Field}
     * specified as part of the white-list in the text file.
     *
     * A single pass is made through each file to collect all the information about each struct, constructor, method,
     * and field.  Most validation will be done at a later point after all white-lists have been gathered and their
     * merging takes place.
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
     *     <li> An augmented method may be specified starting with a Painless type name for the return
     *     type, followed by the fully qualified Java name of the class the augmented method is
     *     part of (this class does not need to be white-listed), the Java name of the method
     *     (which will also be the Painless name for the method), an opening parenthesis, a
     *     comma-delimited list of Painless type names corresponding to the type/class names
     *     for the equivalent Java parameter types (these must be white-listed as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li>A field may be specified starting with a Painless type name for the equivalent Java type
     *     of the field, followed by the Java name of the field (which all be the Painless name
     *     for the field), and a newline. </li>
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
        List<Whitelist.Struct> whitelistStructs = new ArrayList<>();

        // Execute a single pass through the white-list text files.  This will gather all the
        // constructors, methods, augmented methods, and fields for each white-listed struct.
        for (String filepath : filepaths) {
            String line;
            int number = -1;

            try (LineNumberReader reader = new LineNumberReader(
                new InputStreamReader(resource.getResourceAsStream(filepath), StandardCharsets.UTF_8))) {

                String whitelistStructOrigin = null;
                String painlessTypeName = null;
                String javaClassName = null;
                List<Whitelist.Constructor> whitelistConstructors = null;
                List<Whitelist.Method> whitelistMethods = null;
                List<Whitelist.Field> whitelistFields = null;

                while ((line = reader.readLine()) != null) {
                    number = reader.getLineNumber();
                    line = line.trim();

                    // Skip any lines that are either blank or comments.
                    if (line.length() == 0 || line.charAt(0) == '#') {
                        continue;
                    }

                    // Handle a new struct by resetting all the variables necessary to construct a new Whitelist.Struct for the white-list.
                    // Expects the following format: 'class' ID -> ID '{' '\n'
                    if (line.startsWith("class ")) {
                        // Ensure the final token of the line is '{'.
                        if (line.endsWith("{") == false) {
                            throw new IllegalArgumentException(
                                "invalid struct definition: failed to parse class opening bracket [" + line + "]");
                        }

                        // Parse the Painless type name and Java class name.
                        String[] tokens = line.substring(5, line.length() - 1).replaceAll("\\s+", "").split("->");

                        // Ensure the correct number of tokens.
                        if (tokens.length != 2) {
                            throw new IllegalArgumentException("invalid struct definition: failed to parse class name [" + line + "]");
                        }

                        whitelistStructOrigin = "[" + filepath + "]:[" + number + "]";
                        painlessTypeName = tokens[0];
                        javaClassName = tokens[1];

                        // Reset all the constructors, methods, and fields to support a new struct.
                        whitelistConstructors = new ArrayList<>();
                        whitelistMethods = new ArrayList<>();
                        whitelistFields = new ArrayList<>();

                    // Handle the end of a struct, by creating a new Whitelist.Struct with all the previously gathered
                    // constructors, methods, augmented methods, and fields, and adding it to the list of white-listed structs.
                    // Expects the following format: '}' '\n'
                    } else if (line.equals("}")) {
                        if (painlessTypeName == null) {
                            throw new IllegalArgumentException("invalid struct definition: extraneous closing bracket");
                        }

                        whitelistStructs.add(new Whitelist.Struct(whitelistStructOrigin, painlessTypeName, javaClassName,
                            whitelistConstructors, whitelistMethods, whitelistFields));

                        // Set all the variables to null to ensure a new struct definition is found before other parsable values.
                        whitelistStructOrigin = null;
                        painlessTypeName = null;
                        javaClassName = null;
                        whitelistConstructors = null;
                        whitelistMethods = null;
                        whitelistFields = null;

                    // Handle all other valid cases.
                    } else {
                        // Mark the origin of this parsable object.
                        String origin = "[" + filepath + "]:[" + number + "]";

                        // Ensure we have a defined struct before adding any constructors, methods, augmented methods, or fields.
                        if (painlessTypeName == null) {
                            throw new IllegalArgumentException("invalid object definition: expected a class name [" + line + "]");
                        }

                        // Handle the case for a constructor definition.
                        // Expects the following format: '(' ( ID ( ',' ID )* )? ')' '\n'
                        if (line.startsWith("(")) {
                            // Ensure the final token of the line is ')'.
                            if (line.endsWith(")") == false) {
                                throw new IllegalArgumentException(
                                    "invalid constructor definition: expected a closing parenthesis [" + line + "]");
                            }

                            // Parse the constructor parameters.
                            String[] tokens = line.substring(1, line.length() - 1).replaceAll("\\s+", "").split(",");

                            // Handle the case for a constructor with no parameters.
                            if ("".equals(tokens[0])) {
                                tokens = new String[0];
                            }

                            whitelistConstructors.add(new Whitelist.Constructor(origin, Arrays.asList(tokens)));

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

                            String javaMethodName;
                            String javaAugmentedClassName;

                            // Based on the number of tokens, look up the Java method name and if provided the Java augmented class.
                            if (tokens.length == 2) {
                                javaMethodName = tokens[1];
                                javaAugmentedClassName = null;
                            } else if (tokens.length == 3) {
                                javaMethodName = tokens[2];
                                javaAugmentedClassName = tokens[1];
                            } else {
                                throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                            }

                            String painlessReturnTypeName = tokens[0];

                            // Parse the method parameters.
                            tokens = line.substring(parameterIndex + 1, line.length() - 1).replaceAll("\\s+", "").split(",");

                            // Handle the case for a method with no parameters.
                            if ("".equals(tokens[0])) {
                                tokens = new String[0];
                            }

                            whitelistMethods.add(new Whitelist.Method(origin, javaAugmentedClassName, javaMethodName,
                                painlessReturnTypeName, Arrays.asList(tokens)));

                        // Handle the case for a field definition.
                        // Expects the following format: ID ID '\n'
                        } else {
                            // Parse the field tokens.
                            String[] tokens = line.split("\\s+");

                            // Ensure the correct number of tokens.
                            if (tokens.length != 2) {
                                throw new IllegalArgumentException("invalid field definition: unexpected format [" + line + "]");
                            }

                            whitelistFields.add(new Whitelist.Field(origin, tokens[1], tokens[0]));
                        }
                    }
                }

                // Ensure all structs end with a '}' token before the end of the file.
                if (painlessTypeName != null) {
                    throw new IllegalArgumentException("invalid struct definition: expected closing bracket");
                }
            } catch (Exception exception) {
                throw new RuntimeException("error in [" + filepath + "] at line [" + number + "]", exception);
            }
        }

        return new Whitelist(resource.getClassLoader(), whitelistStructs);
    }

    private WhitelistLoader() {}
}
