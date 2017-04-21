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

package org.elasticsearch.plugins;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.URIParameter;
import java.security.UnresolvedPermission;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

class PluginSecurity {

    /**
     * Reads plugin policy, prints/confirms exceptions
     */
    static void readPolicy(PluginInfo info, Path file, Terminal terminal, Supplier<Path> tmpFile, boolean batch) throws IOException {
        PermissionCollection permissions = parsePermissions(terminal, file, tmpFile.get());
        List<Permission> requested = Collections.list(permissions.elements());
        if (requested.isEmpty()) {
            terminal.println(Verbosity.VERBOSE, "plugin has a policy file with no additional permissions");
        } else {

            // sort permissions in a reasonable order
            Collections.sort(requested, new Comparator<Permission>() {
                @Override
                public int compare(Permission o1, Permission o2) {
                    int cmp = o1.getClass().getName().compareTo(o2.getClass().getName());
                    if (cmp == 0) {
                        String name1 = o1.getName();
                        String name2 = o2.getName();
                        if (name1 == null) {
                            name1 = "";
                        }
                        if (name2 == null) {
                            name2 = "";
                        }
                        cmp = name1.compareTo(name2);
                        if (cmp == 0) {
                            String actions1 = o1.getActions();
                            String actions2 = o2.getActions();
                            if (actions1 == null) {
                                actions1 = "";
                            }
                            if (actions2 == null) {
                                actions2 = "";
                            }
                            cmp = actions1.compareTo(actions2);
                        }
                    }
                    return cmp;
                }
            });

            terminal.println(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            terminal.println(Verbosity.NORMAL, "@     WARNING: plugin requires additional permissions     @");
            terminal.println(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            // print all permissions:
            for (Permission permission : requested) {
                terminal.println(Verbosity.NORMAL, "* " + formatPermission(permission));
            }
            terminal.println(Verbosity.NORMAL, "See http://docs.oracle.com/javase/8/docs/technotes/guides/security/permissions.html");
            terminal.println(Verbosity.NORMAL, "for descriptions of what these permissions allow and the associated risks.");
            prompt(terminal, batch);
        }

        if (info.hasNativeController()) {
            terminal.println(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            terminal.println(Verbosity.NORMAL, "@        WARNING: plugin forks a native controller        @");
            terminal.println(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            terminal.println(Verbosity.NORMAL, "This plugin launches a native controller that is not subject to the Java");
            terminal.println(Verbosity.NORMAL, "security manager nor to system call filters.");
            prompt(terminal, batch);
        }
    }

    private static void prompt(final Terminal terminal, final boolean batch) {
        if (!batch) {
            terminal.println(Verbosity.NORMAL, "");
            String text = terminal.readText("Continue with installation? [y/N]");
            if (!text.equalsIgnoreCase("y")) {
                throw new RuntimeException("installation aborted by user");
            }
        }
    }

    /** Format permission type, name, and actions into a string */
    static String formatPermission(Permission permission) {
        StringBuilder sb = new StringBuilder();

        String clazz = null;
        if (permission instanceof UnresolvedPermission) {
            clazz = ((UnresolvedPermission) permission).getUnresolvedType();
        } else {
            clazz = permission.getClass().getName();
        }
        sb.append(clazz);

        String name = null;
        if (permission instanceof UnresolvedPermission) {
            name = ((UnresolvedPermission) permission).getUnresolvedName();
        } else {
            name = permission.getName();
        }
        if (name != null && name.length() > 0) {
            sb.append(' ');
            sb.append(name);
        }

        String actions = null;
        if (permission instanceof UnresolvedPermission) {
            actions = ((UnresolvedPermission) permission).getUnresolvedActions();
        } else {
            actions = permission.getActions();
        }
        if (actions != null && actions.length() > 0) {
            sb.append(' ');
            sb.append(actions);
        }
        return sb.toString();
    }

    /**
     * Parses plugin policy into a set of permissions
     */
    static PermissionCollection parsePermissions(Terminal terminal, Path file, Path tmpDir) throws IOException {
        // create a zero byte file for "comparison"
        // this is necessary because the default policy impl automatically grants two permissions:
        // 1. permission to exitVM (which we ignore)
        // 2. read permission to the code itself (e.g. jar file of the code)

        Path emptyPolicyFile = Files.createTempFile(tmpDir, "empty", "tmp");
        final Policy emptyPolicy;
        try {
            emptyPolicy = Policy.getInstance("JavaPolicy", new URIParameter(emptyPolicyFile.toUri()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        IOUtils.rm(emptyPolicyFile);

        // parse the plugin's policy file into a set of permissions
        final Policy policy;
        try {
            policy = Policy.getInstance("JavaPolicy", new URIParameter(file.toUri()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        PermissionCollection permissions = policy.getPermissions(PluginSecurity.class.getProtectionDomain());
        // this method is supported with the specific implementation we use, but just check for safety.
        if (permissions == Policy.UNSUPPORTED_EMPTY_COLLECTION) {
            throw new UnsupportedOperationException("JavaPolicy implementation does not support retrieving permissions");
        }
        PermissionCollection actualPermissions = new Permissions();
        for (Permission permission : Collections.list(permissions.elements())) {
            if (!emptyPolicy.implies(PluginSecurity.class.getProtectionDomain(), permission)) {
                actualPermissions.add(permission);
            }
        }
        actualPermissions.setReadOnly();
        return actualPermissions;
    }
}
