/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.jdkapi;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.entitlement.tools.AccessibleJdkMethods;
import org.elasticsearch.entitlement.tools.AccessibleJdkMethods.AccessibleMethod;
import org.elasticsearch.entitlement.tools.AccessibleJdkMethods.ModuleClass;
import org.elasticsearch.entitlement.tools.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdkApiExtractor {
    private static final String SEPARATOR = "\t";

    private static String DEPRECATIONS_ONLY = "--deprecations-only";
    private static String INCLUDE_INCUBATOR = "--include-incubator";

    private static Set<String> OPTIONAL_ARGS = Set.of(DEPRECATIONS_ONLY, INCLUDE_INCUBATOR);

    public static void main(String[] args) throws IOException {
        validateArgs(args);
        boolean includeIncubator = optionalArgs(args).anyMatch(INCLUDE_INCUBATOR::equals);
        boolean deprecationsOnly = optionalArgs(args).anyMatch(DEPRECATIONS_ONLY::equals);
        writeFile(Path.of(args[0]), AccessibleJdkMethods.loadAccessibleMethods(Utils.modulePredicate(includeIncubator)), deprecationsOnly);
    }

    private static Stream<String> optionalArgs(String[] args) {
        return Arrays.stream(args).skip(1);
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void validateArgs(String[] args) {
        boolean valid = args.length > 0 && optionalArgs(args).allMatch(OPTIONAL_ARGS::contains);
        if (valid && isWritableOutputPath(args[0]) == false) {
            valid = false;
            System.err.println("invalid output path: " + args[0]);
        }
        if (valid == false) {
            String optionalArgs = OPTIONAL_ARGS.stream().collect(Collectors.joining("] [", " [", "]"));
            System.err.println("usage: <output file path>" + optionalArgs);
            System.exit(1);
        }
    }

    private static boolean isWritableOutputPath(String pathStr) {
        try {
            Path path = Paths.get(pathStr);
            if (Files.exists(path) && Files.isRegularFile(path)) {
                return Files.isWritable(path);
            }
            Path parent = path.toAbsolutePath().getParent();
            return parent != null && Files.isDirectory(parent) && Files.isWritable(parent);
        } catch (Exception e) {
            return false;
        }
    }

    private static CharSequence writeLine(ModuleClass moduleClass, AccessibleMethod method) {
        return String.join(
            SEPARATOR,
            moduleClass.module(),
            "", // compatibility with public-callers-finder
            "", // compatibility with public-callers-finder
            moduleClass.clazz(),
            method.descriptor().method(),
            method.descriptor().descriptor(),
            method.descriptor().isPublic() ? "PUBLIC" : "PROTECTED",
            method.descriptor().isStatic() ? "STATIC" : "",
            method.isFinal() ? "FINAL" : ""
        );
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void writeFile(Path path, Stream<Tuple<ModuleClass, AccessibleMethod>> methods, boolean deprecationsOnly)
        throws IOException {
        System.out.println("Writing result for " + Runtime.version() + " to " + path.toAbsolutePath());
        Predicate<Tuple<ModuleClass, AccessibleMethod>> predicate = deprecationsOnly ? t -> t.v2().isDeprecated() : t -> true;
        Files.write(path, () -> methods.filter(predicate).map(t -> writeLine(t.v1(), t.v2())).iterator(), StandardCharsets.UTF_8);
    }

}
