/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.bootstrap.PluginPolicyInfo;
import org.elasticsearch.bootstrap.PolicyUtil;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.security.Permission;
import java.security.UnresolvedPermission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains methods for displaying extended plugin permissions to the user, and confirming that
 * plugin installation can proceed.
 */
public class PluginSecurity {

    /**
     * prints/confirms policy exceptions with the user
     */
    static void confirmPolicyExceptions(Terminal terminal, Set<String> permissions, boolean batch) throws UserException {
        List<String> requested = new ArrayList<>(permissions);
        if (requested.isEmpty()) {
            terminal.println(Verbosity.VERBOSE, "plugin has a policy file with no additional permissions");
        } else {
            // sort permissions in a reasonable order
            Collections.sort(requested);

            if (terminal.isHeadless()) {
                terminal.errorPrintln(
                    "WARNING: plugin requires additional permissions: ["
                        + requested.stream().map(each -> '\'' + each + '\'').collect(Collectors.joining(", "))
                        + "]"
                );
                terminal.errorPrintln(
                    "See https://docs.oracle.com/javase/8/docs/technotes/guides/security/permissions.html"
                        + " for descriptions of what these permissions allow and the associated risks."
                );
            } else {
                terminal.errorPrintln(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                terminal.errorPrintln(Verbosity.NORMAL, "@     WARNING: plugin requires additional permissions     @");
                terminal.errorPrintln(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                // print all permissions:
                for (String permission : requested) {
                    terminal.errorPrintln(Verbosity.NORMAL, "* " + permission);
                }
                terminal.errorPrintln(
                    Verbosity.NORMAL,
                    "See https://docs.oracle.com/javase/8/docs/technotes/guides/security/permissions.html"
                );
                terminal.errorPrintln(Verbosity.NORMAL, "for descriptions of what these permissions allow and the associated risks.");

                if (batch == false) {
                    prompt(terminal);
                }
            }
        }
    }

    private static void prompt(final Terminal terminal) throws UserException {
        terminal.println(Verbosity.NORMAL, "");
        String text = terminal.readText("Continue with installation? [y/N]");
        if (text.equalsIgnoreCase("y") == false) {
            throw new UserException(ExitCodes.DATA_ERROR, "installation aborted by user");
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
     * Extract a unique set of permissions from the plugin's policy file. Each permission is formatted for output to users.
     */
    public static Set<String> getPermissionDescriptions(PluginPolicyInfo pluginPolicyInfo, Path tmpDir) throws IOException {
        Set<Permission> allPermissions = new HashSet<>(PolicyUtil.getPolicyPermissions(null, pluginPolicyInfo.policy(), tmpDir));
        for (URL jar : pluginPolicyInfo.jars()) {
            Set<Permission> jarPermissions = PolicyUtil.getPolicyPermissions(jar, pluginPolicyInfo.policy(), tmpDir);
            allPermissions.addAll(jarPermissions);
        }

        return allPermissions.stream().map(PluginSecurity::formatPermission).collect(Collectors.toSet());
    }
}
