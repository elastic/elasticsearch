/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.securitymanager.scanner;

import org.elasticsearch.entitlement.tools.Utils;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Main {

    static final Set<String> excludedModules = Set.of("java.desktop");

    private static void identifySMChecksEntryPoints() throws IOException {

        FileSystem fs = FileSystems.getFileSystem(URI.create("jrt:/"));

        var moduleExports = Utils.findModuleExports(fs);

        var callers = new HashMap<String, List<SecurityCheckClassVisitor.CallerInfo>>();
        var visitor = new SecurityCheckClassVisitor(callers);

        try (var stream = Files.walk(fs.getPath("modules"))) {
            stream.filter(x -> x.toString().endsWith(".class")).forEach(x -> {
                var moduleName = x.subpath(1, 2).toString();
                if (excludedModules.contains(moduleName) == false) {
                    try {
                        ClassReader cr = new ClassReader(Files.newInputStream(x));
                        visitor.setCurrentModule(moduleName, moduleExports.get(moduleName));
                        var path = x.getNameCount() > 3 ? x.subpath(2, x.getNameCount() - 1).toString() : "";
                        visitor.setCurrentSourcePath(path);
                        cr.accept(visitor, 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        for (var kv : callers.entrySet()) {
            for (var e : kv.getValue()) {
                System.out.println(toString(kv.getKey(), e));
            }
        }
    }

    private static String toString(String calleeName, SecurityCheckClassVisitor.CallerInfo callerInfo) {
        var s = callerInfo.moduleName()
            + ";"
            + callerInfo.source()
            + ";"
            + callerInfo.line()
            + ";"
            + callerInfo.className()
            + ";"
            + callerInfo.methodName()
            + ";"
            + callerInfo.isPublic();

        if (callerInfo.runtimePermissionType() != null) {
            s += ";" + callerInfo.runtimePermissionType();
        } else if (calleeName.equals("checkPermission")) {
            s += ";MISSING"; // missing information
        } else {
            s += ";" + calleeName;
        }

        if (callerInfo.permissionType() != null) {
            s += ";" + callerInfo.permissionType();
        } else if (calleeName.equals("checkPermission")) {
            s += ";MISSING"; // missing information
        } else {
            s += ";";
        }
        return s;
    }

    public static void main(String[] args) throws IOException {
        identifySMChecksEntryPoints();
    }
}
