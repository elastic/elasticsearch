/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.publiccallersfinder;

import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {

    static final Set<String> excludedModules = Set.of("java.desktop");

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
        for (var oc : firstLevelCallers) {
            var methodsToCheck = new ArrayDeque<>(Set.of(oc));
            var methodsSeen = new HashSet<>();

            while (methodsToCheck.isEmpty() == false) {
                var methodToCheck = methodsToCheck.removeFirst();
                var m = methodToCheck.entryPoint();
                var visitor2 = new FindUsagesClassVisitor(
                    moduleExports,
                    new FindUsagesClassVisitor.MethodDescriptor(m.className(), m.methodName(), m.methodDescriptor()),
                    (source, line, className, methodName, methodDescriptor, isPublic) -> {
                        var newMethod = new CallChain(
                            new FindUsagesClassVisitor.EntryPoint(
                                m.moduleName(),
                                source,
                                line,
                                className,
                                methodName,
                                methodDescriptor,
                                isPublic
                            ),
                            methodToCheck
                        );

                        var notSeenBefore = methodsSeen.add(newMethod.entryPoint());
                        if (notSeenBefore) {
                            if (isPublic) {
                                usageConsumer.usageFound(oc.next(), newMethod);
                            }
                            if (isPublic == false || bubbleUpFromPublic) {
                                methodsToCheck.add(newMethod);
                            }
                        }
                    }
                );

                for (var x : classesToScan) {
                    try {
                        ClassReader cr = new ClassReader(Files.newInputStream(x));
                        cr.accept(visitor2, 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static void identifyTopLevelEntryPoints(FindUsagesClassVisitor.MethodDescriptor methodToFind, boolean bubbleUpFromPublic)
        throws IOException {
        FileSystem fs = FileSystems.getFileSystem(URI.create("jrt:/"));

        var moduleExports = findModuleExports(fs);
        final var separator = '\t';

        try (var stream = Files.walk(fs.getPath("modules"))) {
            var modules = stream.filter(x -> x.toString().endsWith(".class"))
                .collect(Collectors.groupingBy(x -> x.subpath(1, 2).toString()));

            for (var kv : modules.entrySet()) {
                var moduleName = kv.getKey();
                if (excludedModules.contains(moduleName) == false) {
                    var thisModuleExports = moduleExports.get(moduleName);
                    var originalCallers = new ArrayList<CallChain>();
                    var visitor = new FindUsagesClassVisitor(
                        thisModuleExports,
                        methodToFind,
                        (source, line, className, methodName, methodDescriptor, isPublic) -> {
                            originalCallers.add(
                                new CallChain(
                                    new FindUsagesClassVisitor.EntryPoint(
                                        moduleName,
                                        source,
                                        line,
                                        className,
                                        methodName,
                                        methodDescriptor,
                                        isPublic
                                    ),
                                    new CallChain(
                                        new FindUsagesClassVisitor.EntryPoint(
                                            moduleName,
                                            "",
                                            0,
                                            methodToFind.className(),
                                            methodToFind.methodName(),
                                            methodToFind.methodDescriptor(),
                                            true
                                        ),
                                        null
                                    )
                                )
                            );
                        }
                    );

                    for (var x : kv.getValue()) {
                        try {
                            ClassReader cr = new ClassReader(Files.newInputStream(x));
                            cr.accept(visitor, 0);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    originalCallers.stream().filter(c -> c.entryPoint().isPublic()).forEach(c -> {
                        var oc = c.next();
                        var originalEntryPoint = oc.entryPoint().moduleName() + separator + oc.entryPoint().className() + separator + oc
                            .entryPoint()
                            .methodName();
                        var e = c.entryPoint();
                        var entryPoint = e.moduleName() + separator + e.source() + separator + e.line() + separator + e.className()
                            + separator + e.methodName() + separator + e.methodDescriptor();
                        System.out.println(entryPoint + separator + originalEntryPoint);
                    });
                    var firstLevelCallers = bubbleUpFromPublic
                        ? originalCallers
                        : originalCallers.stream().filter(c -> c.entryPoint().isPublic() == false).toList();

                    if (firstLevelCallers.isEmpty() == false) {
                        findTransitiveUsages(firstLevelCallers, kv.getValue(), thisModuleExports, bubbleUpFromPublic, (oc, callChain) -> {
                            var originalEntryPoint = oc.entryPoint().moduleName() + separator + oc.entryPoint().className() + separator + oc
                                .entryPoint()
                                .methodName();
                            var s = moduleName + separator + callChain.entryPoint().source() + separator + callChain.entryPoint().line()
                                + separator + callChain.entryPoint().className() + separator + callChain.entryPoint().methodName()
                                + separator + callChain.entryPoint().methodDescriptor();
                            System.out.println(s + separator + originalEntryPoint);
                        });
                    }
                }
            }
        }
    }

    interface MethodDescriptorConsumer {
        void accept(FindUsagesClassVisitor.MethodDescriptor methodDescriptor) throws IOException;
    }

    private static void parseCsv(Path csvPath, boolean bubbleUpFromPublic, MethodDescriptorConsumer methodConsumer) throws IOException {
        var lines = Files.readAllLines(csvPath);
        for (var l : lines) {
            var tokens = l.split(";");
            var className = tokens[3];
            var methodName = tokens[4];
            var isPublic = Boolean.parseBoolean(tokens[5]);
            if (isPublic == false || bubbleUpFromPublic) {
                methodConsumer.accept(new FindUsagesClassVisitor.MethodDescriptor(className, methodName, null));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        var csvFilePath = Path.of(args[0]);
        boolean bubbleUpFromPublic = args.length >= 2 && Boolean.parseBoolean(args[1]);
        parseCsv(csvFilePath, bubbleUpFromPublic, x -> identifyTopLevelEntryPoints(x, bubbleUpFromPublic));
    }
}
