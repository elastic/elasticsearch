/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.tools.ExternalAccess;
import org.elasticsearch.entitlement.tools.Utils;
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
import java.util.stream.Stream;

public class Main {

    private static final String SEPARATOR = "\t";

    private static final Set<FindUsagesClassVisitor.MethodDescriptor> INSTRUMENTED_METHODS = new HashSet<>();

    record CallChain(FindUsagesClassVisitor.EntryPoint entryPoint, CallChain next) {}

    interface UsageConsumer {
        void usageFound(CallChain originalEntryPoint, CallChain newMethod);
    }

    private static void findTransitiveUsages(
        Collection<CallChain> firstLevelCallers,
        List<Path> classesToScan,
        Set<String> moduleExports,
        boolean bubbleUpFromPublic,
        UsageConsumer usageConsumer
    ) {
        for (var caller : firstLevelCallers) {
            var methodsToCheck = new ArrayDeque<>(Set.of(caller));
            var methodsSeen = new HashSet<FindUsagesClassVisitor.EntryPoint>();

            while (methodsToCheck.isEmpty() == false) {
                var methodToCheck = methodsToCheck.removeFirst();
                var entryPoint = methodToCheck.entryPoint();
                var visitor2 = new FindUsagesClassVisitor(moduleExports, entryPoint.method(), (source, line, method, access) -> {
                    var newMethod = new CallChain(
                        new FindUsagesClassVisitor.EntryPoint(entryPoint.moduleName(), source, line, method, access),
                        methodToCheck
                    );

                    var notSeenBefore = methodsSeen.add(newMethod.entryPoint());
                    if (notSeenBefore) {
                        if (ExternalAccess.isExternallyAccessible(access)) {
                            usageConsumer.usageFound(caller.next(), newMethod);
                        }
                        if (access.contains(ExternalAccess.PUBLIC_METHOD) == false || bubbleUpFromPublic) {
                            methodsToCheck.add(newMethod);
                        }
                    }
                });

                for (var classFile : classesToScan) {
                    try {
                        ClassReader cr = new ClassReader(Files.newInputStream(classFile));
                        cr.accept(visitor2, 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static void identifyTopLevelEntryPoints(
        FindUsagesClassVisitor.MethodDescriptor methodToFind,
        String methodToFindModule,
        EnumSet<ExternalAccess> methodToFindAccess,
        boolean bubbleUpFromPublic
    ) throws IOException {

        Utils.walkJdkModules((moduleName, moduleClasses, moduleExports) -> {
            var originalCallers = new ArrayList<CallChain>();
            var visitor = new FindUsagesClassVisitor(
                moduleExports,
                methodToFind,
                (source, line, method, access) -> originalCallers.add(
                    new CallChain(
                        new FindUsagesClassVisitor.EntryPoint(moduleName, source, line, method, access),
                        new CallChain(
                            new FindUsagesClassVisitor.EntryPoint(methodToFindModule, "", 0, methodToFind, methodToFindAccess),
                            null
                        )
                    )
                )
            );

            for (var classFile : moduleClasses) {
                try {
                    ClassReader cr = new ClassReader(Files.newInputStream(classFile));
                    cr.accept(visitor, 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            originalCallers.stream().filter(c -> ExternalAccess.isExternallyAccessible(c.entryPoint().access())).forEach(c -> {
                var originalCaller = c.next();
                printRow(getEntryPointColumns(c.entryPoint().moduleName(), c.entryPoint()), getOriginalEntryPointColumns(originalCaller));
            });
            var firstLevelCallers = bubbleUpFromPublic ? originalCallers : originalCallers.stream().filter(Main::isNotFullyPublic).toList();

            if (firstLevelCallers.isEmpty() == false) {
                findTransitiveUsages(
                    firstLevelCallers,
                    moduleClasses,
                    moduleExports,
                    bubbleUpFromPublic,
                    (originalEntryPoint, newMethod) -> printRow(
                        getEntryPointColumns(moduleName, newMethod.entryPoint()),
                        getOriginalEntryPointColumns(originalEntryPoint)
                    )
                );
            }
        });
    }

    private static boolean isNotFullyPublic(CallChain c) {
        return (c.entryPoint().access().contains(ExternalAccess.PUBLIC_CLASS)
            && c.entryPoint().access().contains(ExternalAccess.PUBLIC_METHOD)) == false;
    }

    @SuppressForbidden(reason = "This tool prints the CSV to stdout")
    private static void printRow(CharSequence[] entryPointColumns, CharSequence[] originalEntryPointColumns) {
        String row = String.join(
            SEPARATOR,
            () -> Stream.concat(Arrays.stream(entryPointColumns), Arrays.stream(originalEntryPointColumns)).iterator()
        );
        System.out.println(row);
    }

    private static CharSequence[] getEntryPointColumns(String moduleName, FindUsagesClassVisitor.EntryPoint e) {
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
        void accept(FindUsagesClassVisitor.MethodDescriptor methodDescriptor, String moduleName, EnumSet<ExternalAccess> access)
            throws IOException;
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
            methodConsumer.accept(new FindUsagesClassVisitor.MethodDescriptor(className, methodName, methodDescriptor), moduleName, access);
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
            INSTRUMENTED_METHODS.add(new FindUsagesClassVisitor.MethodDescriptor(parts[0], parts[1], parts[2]));
        }
    }

    public static void main(String[] args) throws IOException {
        var csvFilePath = Path.of(args[0]);
        boolean bubbleUpFromPublic = args.length >= 2 && Booleans.parseBoolean(args[1]);

        if (System.getProperty("es.entitlements.dump") != null) {
            loadInstrumentedMethods(Path.of(System.getProperty("es.entitlements.dump")));
        }
        parseCsv(csvFilePath, (method, module, access) -> identifyTopLevelEntryPoints(method, module, access, bubbleUpFromPublic));
    }
}
