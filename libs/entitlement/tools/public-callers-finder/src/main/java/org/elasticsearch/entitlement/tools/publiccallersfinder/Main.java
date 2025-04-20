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
import org.elasticsearch.entitlement.tools.ExternalAccess;
import org.elasticsearch.entitlement.tools.Utils;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {

    private static final String SEPARATOR = "\t";

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
                var m = methodToCheck.entryPoint();
                var visitor2 = new FindUsagesClassVisitor(
                    moduleExports,
                    new FindUsagesClassVisitor.MethodDescriptor(m.className(), m.methodName(), m.methodDescriptor()),
                    (source, line, className, methodName, methodDescriptor, access) -> {
                        var newMethod = new CallChain(
                            new FindUsagesClassVisitor.EntryPoint(
                                m.moduleName(),
                                source,
                                line,
                                className,
                                methodName,
                                methodDescriptor,
                                access
                            ),
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
                    }
                );

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
                (source, line, className, methodName, methodDescriptor, access) -> originalCallers.add(
                    new CallChain(
                        new FindUsagesClassVisitor.EntryPoint(moduleName, source, line, className, methodName, methodDescriptor, access),
                        new CallChain(
                            new FindUsagesClassVisitor.EntryPoint(
                                methodToFindModule,
                                "",
                                0,
                                methodToFind.className(),
                                methodToFind.methodName(),
                                methodToFind.methodDescriptor(),
                                methodToFindAccess
                            ),
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
                printRow(getEntryPointString(c.entryPoint().moduleName(), c.entryPoint()), getOriginalEntryPointString(originalCaller));
            });
            var firstLevelCallers = bubbleUpFromPublic ? originalCallers : originalCallers.stream().filter(Main::isNotFullyPublic).toList();

            if (firstLevelCallers.isEmpty() == false) {
                findTransitiveUsages(
                    firstLevelCallers,
                    moduleClasses,
                    moduleExports,
                    bubbleUpFromPublic,
                    (originalEntryPoint, newMethod) -> printRow(
                        getEntryPointString(moduleName, newMethod.entryPoint()),
                        getOriginalEntryPointString(originalEntryPoint)
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
    private static void printRow(String entryPointString, String originalEntryPoint) {
        System.out.println(entryPointString + SEPARATOR + originalEntryPoint);
    }

    private static String getEntryPointString(String moduleName, FindUsagesClassVisitor.EntryPoint e) {
        return moduleName + SEPARATOR + e.source() + SEPARATOR + e.line() + SEPARATOR + e.className() + SEPARATOR + e.methodName()
            + SEPARATOR + e.methodDescriptor() + SEPARATOR + ExternalAccess.toString(e.access());
    }

    private static String getOriginalEntryPointString(CallChain originalCallChain) {
        return originalCallChain.entryPoint().moduleName() + SEPARATOR + originalCallChain.entryPoint().className() + SEPARATOR
            + originalCallChain.entryPoint().methodName() + SEPARATOR + ExternalAccess.toString(originalCallChain.entryPoint().access());
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

    public static void main(String[] args) throws IOException {
        var csvFilePath = Path.of(args[0]);
        boolean bubbleUpFromPublic = args.length >= 2 && Boolean.parseBoolean(args[1]);
        parseCsv(csvFilePath, (method, module, access) -> identifyTopLevelEntryPoints(method, module, access, bubbleUpFromPublic));
    }
}
