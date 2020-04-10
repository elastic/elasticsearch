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

import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Loads and creates a {@link Whitelist} from one to many text files. */
public final class WhitelistLoader {

    /**
     * Loads and creates a {@link Whitelist} from one to many text files using only the base annotation parsers.
     * See {@link #loadFromResourceFiles(Class, Map, String...)} for information on how to structure a whitelist
     * text file.
     */
    public static Whitelist loadFromResourceFiles(Class<?> resource, String... filepaths) {
        return loadFromResourceFiles(resource, WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS, filepaths);
    }

    /**
     * Loads and creates a {@link Whitelist} from one to many text files. The file paths are passed in as an array of
     * {@link String}s with a single {@link Class} to be be used to load the resources where each {@link String}
     * is the path of a single text file. The {@link Class}'s {@link ClassLoader} will be used to lookup the Java
     * reflection objects for each individual {@link Class}, {@link Constructor}, {@link Method}, and {@link Field}
     * specified as part of the whitelist in the text file.
     *
     * A single pass is made through each file to collect all the information about each class, constructor, method,
     * and field. Most validation will be done at a later point after all whitelists have been gathered and their
     * merging takes place.
     *
     * A painless type name is one of the following:
     * <ul>
     *     <li> def - The Painless dynamic type which is automatically included without a need to be
     *     whitelisted. </li>
     *     <li> fully-qualified Java type name - Any whitelisted Java class will have the equivalent name as
     *     a Painless type name with the exception that any dollar symbols used as part of inner classes will
     *     be replaced with dot symbols. </li>
     *     <li> short Java type name - The text after the final dot symbol of any specified Java class. A
     *     short type Java name may be excluded by using the 'no_import' attribute during Painless class parsing
     *     as described later. </li>
     * </ul>
     *
     * The following can be parsed from each whitelist text file:
     * <ul>
     *   <li> Blank lines will be ignored by the parser. </li>
     *   <li> Comments may be created starting with a pound '#' symbol and end with a newline. These will
     *   be ignored by the parser. </li>
     *   <li> Primitive types may be specified starting with 'class' and followed by the Java type name,
     *   an opening bracket, a newline, a closing bracket, and a final newline. </li>
     *   <li> Complex types may be specified starting with 'class' and followed by the fully-qualified Java
     *   class name, optionally followed by a 'no_import' attribute, an opening bracket, a newline,
     *   constructor/method/field specifications, a closing bracket, and a final newline. Within a complex
     *   type the following may be parsed:
     *   <ul>
     *     <li> A constructor may be specified starting with an opening parenthesis, followed by a
     *     comma-delimited list of Painless type names corresponding to the type/class names for
     *     the equivalent Java parameter types (these must be whitelisted as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li> A method may be specified starting with a Painless type name for the return type,
     *     followed by the Java name of the method (which will also be the Painless name for the
     *     method), an opening parenthesis, a comma-delimited list of Painless type names
     *     corresponding to the type/class names for the equivalent Java parameter types
     *     (these must be whitelisted as well), a closing parenthesis, and a newline. </li>
     *     <li> An augmented method may be specified starting with a Painless type name for the return
     *     type, followed by the fully qualified Java name of the class the augmented method is
     *     part of (this class does not need to be whitelisted), the Java name of the method
     *     (which will also be the Painless name for the method), an opening parenthesis, a
     *     comma-delimited list of Painless type names corresponding to the type/class names
     *     for the equivalent Java parameter types (these must be whitelisted as well), a closing
     *     parenthesis, and a newline. </li>
     *     <li>A field may be specified starting with a Painless type name for the equivalent Java type
     *     of the field, followed by the Java name of the field (which all be the Painless name
     *     for the field), and a newline. </li>
     *   </ul>
     *   <li> Annotations may be added starting with an at, followed by a name, optionally an opening brace,
     *   a parameter name, an equals, an opening quote, an argument value, a closing quote, (possibly repeated
     *   for multiple arguments,) and a closing brace. Multiple annotations may be added after a class (before
     *   the opening bracket), after a method, or after field. </li>
     * </ul>
     *
     * Note there must be a one-to-one correspondence of Painless type names to Java type/class names.
     * If the same Painless type is defined across multiple files and the Java class is the same, all
     * specified constructors, methods, and fields will be merged into a single Painless type. The
     * Painless dynamic type, 'def', used as part of constructor, method, and field definitions will
     * be appropriately parsed and handled. Painless complex types must be specified with the
     * fully-qualified Java class name. Method argument types, method return types, and field types
     * must be specified with Painless type names (def, fully-qualified, or short) as described earlier.
     *
     * The following example is used to create a single whitelist text file:
     *
     * {@code
     * # primitive types
     *
     * class int -> int {
     * }
     *
     * # complex types
     *
     * class my.package.Example @no_import {
     *   # constructors
     *   ()
     *   (int)
     *   (def, def)
     *   (Example, def)
     *
     *   # method
     *   Example add(int, def)
     *   int add(Example, Example)
     *   void example() @deprecated[use example 2 instead]
     *   void example2()
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
    public static Whitelist loadFromResourceFiles(Class<?> resource, Map<String, WhitelistAnnotationParser> parsers, String... filepaths) {
        List<WhitelistClass> whitelistClasses = new ArrayList<>();
        List<WhitelistMethod> whitelistStatics = new ArrayList<>();
        List<WhitelistClassBinding> whitelistClassBindings = new ArrayList<>();

        // Execute a single pass through the whitelist text files.  This will gather all the
        // constructors, methods, augmented methods, and fields for each whitelisted class.
        for (String filepath : filepaths) {
            String line;
            int number = -1;

            try (LineNumberReader reader = new LineNumberReader(
                    new InputStreamReader(resource.getResourceAsStream(filepath), StandardCharsets.UTF_8))) {

                String parseType = null;
                String whitelistClassOrigin = null;
                String javaClassName = null;
                List<WhitelistConstructor> whitelistConstructors = null;
                List<WhitelistMethod> whitelistMethods = null;
                List<WhitelistField> whitelistFields = null;
                List<Object> classAnnotations = null;

                while ((line = reader.readLine()) != null) {
                    number = reader.getLineNumber();
                    line = line.trim();

                    // Skip any lines that are either blank or comments.
                    if (line.length() == 0 || line.charAt(0) == '#') {
                        continue;
                    }

                    // Handle a new class by resetting all the variables necessary to construct a new WhitelistClass for the whitelist.
                    // Expects the following format: 'class' ID annotations? '{' '\n'
                    if (line.startsWith("class ")) {
                        // Ensure the final token of the line is '{'.
                        if (line.endsWith("{") == false) {
                            throw new IllegalArgumentException(
                                    "invalid class definition: failed to parse class opening bracket [" + line + "]");
                        }

                        if (parseType != null) {
                            throw new IllegalArgumentException("invalid definition: cannot embed class definition [" + line + "]");
                        }

                        // Parse the Java class name and annotations if they exist.
                        int annotationIndex = line.indexOf('@');

                        if (annotationIndex == -1) {
                            annotationIndex = line.length() - 1;
                            classAnnotations = Collections.emptyList();
                        } else {
                            classAnnotations = parseWhitelistAnnotations(parsers, line.substring(annotationIndex, line.length() - 1));
                        }

                        parseType = "class";
                        whitelistClassOrigin = "[" + filepath + "]:[" + number + "]";
                        javaClassName = line.substring(5, annotationIndex).trim();

                        // Reset all the constructors, methods, and fields to support a new class.
                        whitelistConstructors = new ArrayList<>();
                        whitelistMethods = new ArrayList<>();
                        whitelistFields = new ArrayList<>();
                    } else if (line.startsWith("static_import ")) {
                        // Ensure the final token of the line is '{'.
                        if (line.endsWith("{") == false) {
                            throw new IllegalArgumentException(
                                    "invalid static import definition: failed to parse static import opening bracket [" + line + "]");
                        }

                        if (parseType != null) {
                            throw new IllegalArgumentException("invalid definition: cannot embed static import definition [" + line + "]");
                        }

                        parseType = "static_import";

                    // Handle the end of a definition and reset all previously gathered values.
                    // Expects the following format: '}' '\n'
                    } else if (line.equals("}")) {
                        if (parseType == null) {
                            throw new IllegalArgumentException("invalid definition: extraneous closing bracket");
                        }

                        // Create a new WhitelistClass with all the previously gathered constructors, methods,
                        // augmented methods, and fields, and add it to the list of whitelisted classes.
                        if ("class".equals(parseType)) {
                            whitelistClasses.add(new WhitelistClass(whitelistClassOrigin, javaClassName,
                                    whitelistConstructors, whitelistMethods, whitelistFields, classAnnotations));

                            whitelistClassOrigin = null;
                            javaClassName = null;
                            whitelistConstructors = null;
                            whitelistMethods = null;
                            whitelistFields = null;
                            classAnnotations = null;
                        }

                        // Reset the parseType.
                        parseType = null;

                    // Handle static import definition types.
                    // Expects the following format: ID ID '(' ( ID ( ',' ID )* )? ')' ( 'from_class' | 'bound_to' ) ID annotations? '\n'
                    } else if ("static_import".equals(parseType)) {
                        // Mark the origin of this parsable object.
                        String origin = "[" + filepath + "]:[" + number + "]";

                        // Parse the tokens prior to the method parameters.
                        int parameterStartIndex = line.indexOf('(');

                        if (parameterStartIndex == -1) {
                            throw new IllegalArgumentException(
                                    "illegal static import definition: start of method parameters not found [" + line + "]");
                        }

                        String[] tokens = line.substring(0, parameterStartIndex).trim().split("\\s+");

                        String methodName;

                        // Based on the number of tokens, look up the Java method name.
                        if (tokens.length == 2) {
                            methodName = tokens[1];
                        } else {
                            throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                        }

                        String returnCanonicalTypeName = tokens[0];

                        // Parse the method parameters.
                        int parameterEndIndex = line.indexOf(')');

                        if (parameterEndIndex == -1) {
                            throw new IllegalArgumentException(
                                    "illegal static import definition: end of method parameters not found [" + line + "]");
                        }

                        String[] canonicalTypeNameParameters =
                                line.substring(parameterStartIndex + 1, parameterEndIndex).replaceAll("\\s+", "").split(",");

                        // Handle the case for a method with no parameters.
                        if ("".equals(canonicalTypeNameParameters[0])) {
                            canonicalTypeNameParameters = new String[0];
                        }

                        // Parse the annotations if they exist.
                        List<Object> annotations;
                        int annotationIndex = line.indexOf('@');

                        if (annotationIndex == -1) {
                            annotationIndex = line.length();
                            annotations = Collections.emptyList();
                        } else {
                            annotations = parseWhitelistAnnotations(parsers, line.substring(annotationIndex));
                        }

                        // Parse the static import type and class.
                        tokens = line.substring(parameterEndIndex + 1, annotationIndex).trim().split("\\s+");

                        String staticImportType;
                        String targetJavaClassName;

                        // Based on the number of tokens, look up the type and class.
                        if (tokens.length == 2) {
                            staticImportType = tokens[0];
                            targetJavaClassName = tokens[1];
                        } else {
                            throw new IllegalArgumentException("invalid static import definition: unexpected format [" + line + "]");
                        }

                        // Add a static import method or binding depending on the static import type.
                        if ("from_class".equals(staticImportType)) {
                            whitelistStatics.add(new WhitelistMethod(origin, targetJavaClassName,
                                    methodName, returnCanonicalTypeName, Arrays.asList(canonicalTypeNameParameters),
                                    annotations));
                        } else if ("bound_to".equals(staticImportType)) {
                            whitelistClassBindings.add(new WhitelistClassBinding(origin, targetJavaClassName,
                                    methodName, returnCanonicalTypeName, Arrays.asList(canonicalTypeNameParameters),
                                    annotations));
                        } else {
                            throw new IllegalArgumentException("invalid static import definition: " +
                                    "unexpected static import type [" + staticImportType + "] [" + line + "]");
                        }

                    // Handle class definition types.
                    } else if ("class".equals(parseType)) {
                        // Mark the origin of this parsable object.
                        String origin = "[" + filepath + "]:[" + number + "]";

                        // Handle the case for a constructor definition.
                        // Expects the following format: '(' ( ID ( ',' ID )* )? ')' annotations? '\n'
                        if (line.startsWith("(")) {
                            // Parse the constructor parameters.
                            int parameterEndIndex = line.indexOf(')');

                            if (parameterEndIndex == -1) {
                                throw new IllegalArgumentException(
                                        "illegal constructor definition: end of constructor parameters not found [" + line + "]");
                            }

                            String[] canonicalTypeNameParameters = line.substring(1, parameterEndIndex).replaceAll("\\s+", "").split(",");

                            // Handle the case for a constructor with no parameters.
                            if ("".equals(canonicalTypeNameParameters[0])) {
                                canonicalTypeNameParameters = new String[0];
                            }

                            // Parse the annotations if they exist.
                            List<Object> annotations;
                            int annotationIndex = line.indexOf('@');
                            annotations = annotationIndex == -1 ?
                                    Collections.emptyList() : parseWhitelistAnnotations(parsers, line.substring(annotationIndex));

                            whitelistConstructors.add(new WhitelistConstructor(
                                    origin, Arrays.asList(canonicalTypeNameParameters), annotations));

                        // Handle the case for a method or augmented method definition.
                        // Expects the following format: ID ID? ID '(' ( ID ( ',' ID )* )? ')' annotations? '\n'
                        } else if (line.contains("(")) {
                            // Parse the tokens prior to the method parameters.
                            int parameterStartIndex = line.indexOf('(');
                            String[] tokens = line.substring(0, parameterStartIndex).trim().split("\\s+");

                            String methodName;
                            String javaAugmentedClassName;

                            // Based on the number of tokens, look up the Java method name and if provided the Java augmented class.
                            if (tokens.length == 2) {
                                methodName = tokens[1];
                                javaAugmentedClassName = null;
                            } else if (tokens.length == 3) {
                                methodName = tokens[2];
                                javaAugmentedClassName = tokens[1];
                            } else {
                                throw new IllegalArgumentException("invalid method definition: unexpected format [" + line + "]");
                            }

                            String returnCanonicalTypeName = tokens[0];

                            // Parse the method parameters.
                            int parameterEndIndex = line.indexOf(')');

                            if (parameterEndIndex == -1) {
                                throw new IllegalArgumentException(
                                        "illegal static import definition: end of method parameters not found [" + line + "]");
                            }

                            String[] canonicalTypeNameParameters =
                                    line.substring(parameterStartIndex + 1, parameterEndIndex).replaceAll("\\s+", "").split(",");

                            // Handle the case for a method with no parameters.
                            if ("".equals(canonicalTypeNameParameters[0])) {
                                canonicalTypeNameParameters = new String[0];
                            }

                            // Parse the annotations if they exist.
                            List<Object> annotations;
                            int annotationIndex = line.indexOf('@');
                            annotations = annotationIndex == -1 ?
                                    Collections.emptyList() : parseWhitelistAnnotations(parsers, line.substring(annotationIndex));

                            whitelistMethods.add(new WhitelistMethod(origin, javaAugmentedClassName, methodName,
                                    returnCanonicalTypeName, Arrays.asList(canonicalTypeNameParameters),
                                    annotations));

                        // Handle the case for a field definition.
                        // Expects the following format: ID ID annotations? '\n'
                        } else {
                            // Parse the annotations if they exist.
                            List<Object> annotations;
                            int annotationIndex = line.indexOf('@');

                            if (annotationIndex == -1) {
                                annotationIndex = line.length();
                                annotations = Collections.emptyList();
                            } else {
                                annotations = parseWhitelistAnnotations(parsers, line.substring(annotationIndex));
                            }

                            // Parse the field tokens.
                            String[] tokens = line.substring(0, annotationIndex).split("\\s+");

                            // Ensure the correct number of tokens.
                            if (tokens.length != 2) {
                                throw new IllegalArgumentException("invalid field definition: unexpected format [" + line + "]");
                            }

                            whitelistFields.add(new WhitelistField(origin, tokens[1], tokens[0], annotations));
                        }
                    } else {
                        throw new IllegalArgumentException("invalid definition: unable to parse line [" + line + "]");
                    }
                }

                // Ensure all classes end with a '}' token before the end of the file.
                if (javaClassName != null) {
                    throw new IllegalArgumentException("invalid definition: expected closing bracket");
                }
            } catch (Exception exception) {
                throw new RuntimeException("error in [" + filepath + "] at line [" + number + "]", exception);
            }
        }

        ClassLoader loader = AccessController.doPrivileged((PrivilegedAction<ClassLoader>)resource::getClassLoader);

        return new Whitelist(loader, whitelistClasses, whitelistStatics, whitelistClassBindings, Collections.emptyList());
    }

    private static List<Object> parseWhitelistAnnotations(
            Map<String, WhitelistAnnotationParser> parsers, String line) {

        List<Object> annotations;

        if (line.isBlank()) {
            annotations = Collections.emptyList();
        } else {
            line = line.trim();

            if (line.charAt(0) != '@') {
                throw new IllegalArgumentException("invalid annotation: expected at symbol [" + line + "]");
            }

            if (line.length() < 2) {
                throw new IllegalArgumentException("invalid annotation: expected name [" + line + "]");
            }

            String[] annotationStrings = line.substring(1).split("@");
            annotations = new ArrayList<>(annotationStrings.length);

            for (String annotationString : annotationStrings) {
                String name;
                Map<String, String> arguments;

                annotationString = annotationString.trim();
                int index = annotationString.indexOf('[');

                if (index == -1) {
                    name = annotationString;
                    arguments = Collections.emptyMap();
                } else {
                    if (annotationString.charAt(annotationString.length() - 1) != ']') {
                        throw new IllegalArgumentException("invalid annotation: expected closing brace [" + line + "]");
                    }

                    name = annotationString.substring(0, index);
                    arguments = new HashMap<>();

                    String[] argumentsStrings = annotationString.substring(index + 1, annotationString.length() - 1).split(",");

                    for (String argumentString : argumentsStrings) {
                        String[] argumentKeyValue = argumentString.split("=");

                        if (argumentKeyValue.length != 2) {
                            throw new IllegalArgumentException("invalid annotation: expected key=\"value\" [" + line + "]");
                        }
                        
                        String argumentKey = argumentKeyValue[0].trim();
                        
                        if (argumentKey.isEmpty()) {
                            throw new IllegalArgumentException("invalid annotation: expected key=\"value\" [" + line + "]");
                        }
                        
                        String argumentValue = argumentKeyValue[1];
                        
                        if (argumentValue.length() < 3 || argumentValue.charAt(0) != '"' || 
                                argumentValue.charAt(argumentValue.length() - 1) != '"') {
                            throw new IllegalArgumentException("invalid annotation: expected key=\"value\" [" + line + "]");
                        }
                        
                        argumentValue = argumentValue.substring(1, argumentValue.length() - 1);

                        arguments.put(argumentKey, argumentValue);
                    }
                }

                WhitelistAnnotationParser parser = parsers.get(name);

                if (parser == null) {
                    throw new IllegalArgumentException("invalid annotation: parser not found for [" + name + "] [" + line + "]");
                }

                annotations.add(parser.parse(arguments));
            }
        }

        return annotations;
    }

    private WhitelistLoader() {}
}
