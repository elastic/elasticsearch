/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeValidationException;

import java.io.PrintStream;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Security;

/**
 * This class starts elasticsearch.
 */
class Elasticsearch {

    /**
     * Main entry point for starting elasticsearch
     */
    public static void main(final String[] args) throws Exception {
        overrideDnsCachePolicyProperties();
        org.elasticsearch.bootstrap.Security.prepopulateSecurityCaller();

        /*
         * We want the JVM to think there is a security manager installed so that if internal policy decisions that would be based on the
         * presence of a security manager or lack thereof act as if there is a security manager present (e.g., DNS cache policy). This
         * forces such policies to take effect immediately.
         */
        org.elasticsearch.bootstrap.Security.setSecurityManager(new SecurityManager() {

            @Override
            public void checkPermission(Permission perm) {
                // grant all permissions so that we can later set the security manager to the one that we want
            }

        });
        LogConfigurator.registerErrorListener();
        final ServerArgs serverArgs;
        var in = new InputStreamStreamInput(System.in);
        serverArgs = new ServerArgs(in);

        final Elasticsearch elasticsearch = new Elasticsearch();
        System.out.println("RUNNING ELASTICSEARCH");
        System.out.println(serverArgs);
        PrintStream err = System.err;
        int exitCode = 0;
        try {
            elasticsearch.init(
                serverArgs.daemonize(),
                serverArgs.pidFile(),
                false,
                new Environment(serverArgs.nodeSettings(), serverArgs.configDir())
            );
        } catch (NodeValidationException e) {
            exitCode = ExitCodes.CONFIG;
            err.print('\24');
            err.println(e.getMessage());
        } catch (UserException e) {
            exitCode = e.exitCode;
            err.print('\24');
            err.println(e.getMessage());
        }
        if (exitCode != ExitCodes.OK) {
            printLogsSuggestion(err);
            System.exit(exitCode);
        }
    }

    /**
     * Prints a message directing the user to look at the logs. A message is only printed if
     * logging has been configured.
     */
    static void printLogsSuggestion(PrintStream err) {
        final String basePath = System.getProperty("es.logs.base_path");
        // It's possible to fail before logging has been configured, in which case there's no point
        // suggesting that the user look in the log file.
        if (basePath != null) {
            err.println(
                "ERROR: Elasticsearch did not exit normally - check the logs at "
                    + basePath
                    + System.getProperty("file.separator")
                    + System.getProperty("es.logs.cluster_name")
                    + ".log"
            );
        }
    }

    private static void overrideDnsCachePolicyProperties() {
        for (final String property : new String[] { "networkaddress.cache.ttl", "networkaddress.cache.negative.ttl" }) {
            final String overrideProperty = "es." + property;
            final String overrideValue = System.getProperty(overrideProperty);
            if (overrideValue != null) {
                try {
                    // round-trip the property to an integer and back to a string to ensure that it parses properly
                    Security.setProperty(property, Integer.toString(Integer.valueOf(overrideValue)));
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("failed to parse [" + overrideProperty + "] with value [" + overrideValue + "]", e);
                }
            }
        }
    }

    void init(final boolean daemonize, final Path pidFile, final boolean quiet, Environment initialEnv) throws NodeValidationException,
        UserException {
        try {
            Bootstrap.init(daemonize == false, pidFile, quiet, initialEnv);
        } catch (BootstrapException | RuntimeException e) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupException(e);
        }
    }
}
