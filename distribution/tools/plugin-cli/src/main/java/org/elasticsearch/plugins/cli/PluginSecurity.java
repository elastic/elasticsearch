/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains methods for displaying extended plugin entitlements to the user, and confirming that
 * plugin installation can proceed.
 */
public class PluginSecurity {

    public static final String ENTITLEMENTS_DESCRIPTION_URL =
        "https://www.elastic.co/guide/en/elasticsearch/plugins/current/creating-classic-plugins.html";

    /**
     * prints/confirms policy exceptions with the user
     */
    static void confirmPolicyExceptions(Terminal terminal, Set<String> entitlements, boolean batch) throws UserException {
        List<String> requested = new ArrayList<>(entitlements);
        if (requested.isEmpty()) {
            terminal.println(
                Verbosity.NORMAL,
                "WARNING: plugin has a policy file with no additional entitlements. Double check this is intentional."
            );
        } else {
            // sort entitlements in a reasonable order
            Collections.sort(requested);

            if (terminal.isHeadless()) {
                terminal.errorPrintln(
                    "WARNING: plugin requires additional entitlements: ["
                        + requested.stream().map(each -> '\'' + each + '\'').collect(Collectors.joining(", "))
                        + "]"
                );
                terminal.errorPrintln(
                    "See " + ENTITLEMENTS_DESCRIPTION_URL + " for descriptions of what these entitlements allow and the associated risks."
                );
            } else {
                terminal.errorPrintln(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                terminal.errorPrintln(Verbosity.NORMAL, "@     WARNING: plugin requires additional entitlements    @");
                terminal.errorPrintln(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                // print all entitlements:
                for (String entitlement : requested) {
                    terminal.errorPrintln(Verbosity.NORMAL, "* " + entitlement);
                }
                terminal.errorPrintln(Verbosity.NORMAL, "See " + ENTITLEMENTS_DESCRIPTION_URL);
                terminal.errorPrintln(Verbosity.NORMAL, "for descriptions of what these entitlements allow and the associated risks.");

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
}
