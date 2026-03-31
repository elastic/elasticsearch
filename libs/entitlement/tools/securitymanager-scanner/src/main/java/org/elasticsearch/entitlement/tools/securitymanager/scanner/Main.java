/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools.securitymanager.scanner;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.tools.ExternalAccess;
import org.elasticsearch.entitlement.tools.Utils;
import org.objectweb.asm.ClassReader;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;

public class Main {

    private static void identifySMChecksEntryPoints() throws IOException {

        var callers = new HashMap<String, List<SecurityCheckClassVisitor.CallerInfo>>();
        var visitor = new SecurityCheckClassVisitor(callers);

        Utils.walkJdkModules((moduleName, moduleClasses, moduleExports) -> {
            for (var classFile : moduleClasses) {
                try {
                    ClassReader cr = new ClassReader(Files.newInputStream(classFile));
                    visitor.setCurrentModule(moduleName, moduleExports);
                    var path = classFile.getNameCount() > 3 ? classFile.subpath(2, classFile.getNameCount() - 1).toString() : "";
                    visitor.setCurrentSourcePath(path);
                    cr.accept(visitor, 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        printToStdout(callers);
    }

    @SuppressForbidden(reason = "This simple tool just prints to System.out")
    private static void printToStdout(HashMap<String, List<SecurityCheckClassVisitor.CallerInfo>> callers) {
        for (var kv : callers.entrySet()) {
            for (var e : kv.getValue()) {
                System.out.println(toString(kv.getKey(), e));
            }
        }
    }

    private static final String SEPARATOR = "\t";

    private static String toString(String calleeName, SecurityCheckClassVisitor.CallerInfo callerInfo) {
        var s = callerInfo.moduleName() + SEPARATOR + callerInfo.source() + SEPARATOR + callerInfo.line() + SEPARATOR + callerInfo
            .className() + SEPARATOR + callerInfo.methodName() + SEPARATOR + callerInfo.methodDescriptor() + SEPARATOR + ExternalAccess
                .toString(callerInfo.externalAccess());

        if (callerInfo.runtimePermissionType() != null) {
            s += SEPARATOR + callerInfo.runtimePermissionType();
        } else if (calleeName.equals("checkPermission")) {
            s += SEPARATOR + "MISSING"; // missing information
        } else {
            s += SEPARATOR + calleeName;
        }

        if (callerInfo.permissionType() != null) {
            s += SEPARATOR + callerInfo.permissionType();
        } else if (calleeName.equals("checkPermission")) {
            s += SEPARATOR + "MISSING"; // missing information
        } else {
            s += SEPARATOR;
        }
        return s;
    }

    public static void main(String[] args) throws IOException {
        identifySMChecksEntryPoints();
    }
}
