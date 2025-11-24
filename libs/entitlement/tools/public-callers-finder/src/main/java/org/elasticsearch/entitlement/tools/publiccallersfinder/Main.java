/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.tools.AccessibleJdkMethods;
import org.elasticsearch.entitlement.tools.ExternalAccess;
import org.elasticsearch.entitlement.tools.Utils;
import org.elasticsearch.entitlement.tools.publiccallersfinder.FindUsagesClassVisitor.EntryPoint;
import org.elasticsearch.entitlement.tools.publiccallersfinder.FindUsagesClassVisitor.MethodDescriptor;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.function.Predicate.not;

public class Main {

    private static final String SEPARATOR = "\t";

    private static String TRANSITIVE = "--transitive";
    private static String CHECK_INSTRUMENTATION = "--check-instrumentation";
    private static String INCLUDE_INCUBATOR = "--include-incubator";

    private static Set<String> OPTIONAL_ARGS = Set.of(TRANSITIVE, CHECK_INSTRUMENTATION, INCLUDE_INCUBATOR);

    private static final Set<MethodDescriptor> INSTRUMENTED_METHODS = new HashSet<>();
    private static final Set<MethodDescriptor> ACCESSIBLE_JDK_METHODS = new HashSet<>();

    record CallChain(EntryPoint entryPoint, CallChain next) {
        boolean isPublic() {
            return ExternalAccess.isExternallyAccessible(entryPoint.access());
        }

        CallChain prepend(MethodDescriptor method, EnumSet<ExternalAccess> access, String module, String source, int line) {
            return new CallChain(new EntryPoint(module, source, line, method, access), this);
        }

        static CallChain firstLevel(MethodDescriptor method, EnumSet<ExternalAccess> access, String module, String source, int line) {
            return new CallChain(new EntryPoint(module, source, line, method, access), null);
        }
    }

    interface UsageConsumer {
        void usageFound(CallChain originalEntryPoint, CallChain newMethod);
    }

    private static void visitClasses(FindUsagesClassVisitor visitor, List<Path> classes) {
        for (var clazz : classes) {
            try {
                ClassReader cr = new ClassReader(Files.newInputStream(clazz));
                cr.accept(visitor, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void findTransitiveUsages(
        Collection<CallChain> firstLevelCallers,
        List<Path> classesToScan,
        boolean bubbleUpFromPublic,
        UsageConsumer usageConsumer
    ) {
        for (var caller : firstLevelCallers) {
            var methodsToCheck = new ArrayDeque<>(Set.of(caller));
            var methodsSeen = new HashSet<EntryPoint>();

            while (methodsToCheck.isEmpty() == false) {
                var methodToCheck = methodsToCheck.removeFirst();
                var entryPoint = methodToCheck.entryPoint();
                var visitor2 = new FindUsagesClassVisitor(
                    entryPoint.method(),
                    ACCESSIBLE_JDK_METHODS::contains,
                    (source, line, method, access) -> {
                        var newMethod = methodToCheck.prepend(method, access, entryPoint.moduleName(), source, line);
                        var notSeenBefore = methodsSeen.add(newMethod.entryPoint());
                        if (notSeenBefore) {
                            if (newMethod.isPublic()) {
                                usageConsumer.usageFound(caller.next(), newMethod);
                            }
                            if (bubbleUpFromPublic || newMethod.isPublic() == false) {
                                methodsToCheck.add(newMethod);
                            }
                        }
                    }
                );
                visitClasses(visitor2, classesToScan);
            }
        }
    }

    private static void identifyTopLevelEntryPoints(
        Predicate<String> modulePredicate,
        MethodDescriptor methodToFind,
        String methodToFindModule,
        EnumSet<ExternalAccess> methodToFindAccess,
        boolean bubbleUpFromPublic
    ) throws IOException {

        CallChain firstLevel = CallChain.firstLevel(methodToFind, methodToFindAccess, methodToFindModule, "", 0);
        Utils.walkJdkModules(modulePredicate, emptyMap(), (moduleName, moduleClasses, ignore) -> {
            var originalCallers = new ArrayList<CallChain>();
            var visitor = new FindUsagesClassVisitor(
                methodToFind,
                ACCESSIBLE_JDK_METHODS::contains,
                (source, line, method, access) -> originalCallers.add(firstLevel.prepend(method, access, moduleName, source, line))
            );
            visitClasses(visitor, moduleClasses);

            originalCallers.stream().filter(CallChain::isPublic).forEach(c -> {
                var originalCaller = c.next();
                printRow(getEntryPointColumns(c.entryPoint().moduleName(), c.entryPoint()), getOriginalEntryPointColumns(originalCaller));
            });
            var firstLevelCallers = bubbleUpFromPublic
                ? originalCallers
                : originalCallers.stream().filter(not(CallChain::isPublic)).toList();

            if (firstLevelCallers.isEmpty() == false) {
                findTransitiveUsages(
                    firstLevelCallers,
                    moduleClasses,
                    bubbleUpFromPublic,
                    (originalEntryPoint, newMethod) -> printRow(
                        getEntryPointColumns(moduleName, newMethod.entryPoint()),
                        getOriginalEntryPointColumns(originalEntryPoint)
                    )
                );
            }
        });
    }

    @SuppressForbidden(reason = "This tool prints the CSV to stdout")
    private static void printRow(CharSequence[] entryPointColumns, CharSequence[] originalEntryPointColumns) {
        String row = String.join(
            SEPARATOR,
            () -> Stream.concat(Arrays.stream(entryPointColumns), Arrays.stream(originalEntryPointColumns)).iterator()
        );
        System.out.println(row);
    }

    private static CharSequence[] getEntryPointColumns(String moduleName, EntryPoint e) {
        if (INSTRUMENTED_METHODS.isEmpty()) {
            return new CharSequence[] {
                moduleName,
                e.source(),
                Integer.toString(e.line()),
                e.method().className(),
                e.method().methodName(),
                e.method().methodDescriptor(),
                ExternalAccess.toString(e.access()) };
        } else {
            return new CharSequence[] {
                moduleName,
                e.source(),
                Integer.toString(e.line()),
                e.method().className(),
                e.method().methodName(),
                e.method().methodDescriptor(),
                ExternalAccess.toString(e.access()),
                INSTRUMENTED_METHODS.contains(e.method()) ? "COVERED" : "MISSING" };
        }
    }

    private static CharSequence[] getOriginalEntryPointColumns(CallChain originalCallChain) {
        return new CharSequence[] {
            originalCallChain.entryPoint().moduleName(),
            originalCallChain.entryPoint().method().className(),
            originalCallChain.entryPoint().method().methodName(),
            ExternalAccess.toString(originalCallChain.entryPoint().access()) };
    }

    interface MethodDescriptorConsumer {
        void accept(MethodDescriptor methodDescriptor, String moduleName, EnumSet<ExternalAccess> access) throws IOException;
    }

    private static void parseCsv(Path csvPath, MethodDescriptorConsumer methodConsumer) throws IOException {
        var lines = Files.readAllLines(csvPath);
        for (var l : lines) {
            var tokens = l.split(SEPARATOR);
            var moduleName = tokens[0];
            var className = tokens[3];
            var methodName = tokens[4];
            var methodDescriptor = tokens[5];
            var access = ExternalAccess.fromString(tokens[6]);
            methodConsumer.accept(new MethodDescriptor(className, methodName, methodDescriptor), moduleName, access);
        }
    }

    private static void loadInstrumentedMethods(Path dump) throws IOException {
        if (dump.isAbsolute() == false) {
            throw new IllegalArgumentException("path to entitlements dump must be absolute");
        }
        var lines = Files.readAllLines(dump);
        for (var l : lines) {
            var parts = l.split(SEPARATOR);
            if (parts.length != 3) {
                throw new IllegalStateException("Invalid line in entitlements dump: " + Arrays.toString(parts));
            }
            INSTRUMENTED_METHODS.add(new MethodDescriptor(parts[0], parts[1], parts[2]));
        }
    }

    private static Stream<String> optionalArgs(String[] args) {
        return Arrays.stream(args).skip(1);
    }

    @SuppressForbidden(reason = "cli tool printing to standard err/out")
    private static void validateArgs(String[] args) {
        boolean valid = args.length > 0 && optionalArgs(args).allMatch(OPTIONAL_ARGS::contains);
        if (valid && Path.of(args[0]).toFile().exists() == false) {
            valid = false;
            System.err.println("invalid input file: " + args[0]);
        }
        if (valid == false) {
            String optionalArgs = OPTIONAL_ARGS.stream().collect(Collectors.joining("] [", " [", "]"));
            System.err.println("usage: <input file>" + optionalArgs);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException {
        validateArgs(args);
        var csvFilePath = Path.of(args[0]);
        boolean bubbleUpFromPublic = optionalArgs(args).anyMatch(TRANSITIVE::equals);
        boolean checkInstrumentation = optionalArgs(args).anyMatch(CHECK_INSTRUMENTATION::equals);
        boolean includeIncubator = optionalArgs(args).anyMatch(INCLUDE_INCUBATOR::equals);

        AccessibleJdkMethods.loadAccessibleMethods(Utils.DEFAULT_MODULE_PREDICATE)
            .forEach(
                t -> ACCESSIBLE_JDK_METHODS.add(
                    new MethodDescriptor(t.v1().clazz(), t.v2().descriptor().method(), t.v2().descriptor().descriptor())
                )
            );

        if (checkInstrumentation && System.getProperty("es.entitlements.dump") != null) {
            loadInstrumentedMethods(Path.of(System.getProperty("es.entitlements.dump")));
        }
        Predicate<String> modulePredicate = Utils.modulePredicate(includeIncubator);
        parseCsv(
            csvFilePath,
            (method, module, access) -> identifyTopLevelEntryPoints(modulePredicate, method, module, access, bubbleUpFromPublic)
        );
    }
}
