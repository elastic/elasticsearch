/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeValidationException;

import java.io.PrintStream;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Security;

import static org.elasticsearch.bootstrap.BootstrapInfo.USER_EXCEPTION_MARKER;

/**
 * This class starts elasticsearch.
 */
class Elasticsearch {

    /**
     * Main entry point for starting elasticsearch
     */
    public static void main(final String[] args) {
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

        final Elasticsearch elasticsearch = new Elasticsearch();
        PrintStream err = System.err;
        int exitCode = 0;
        try {
            final var in = new InputStreamStreamInput(System.in);
            final ServerArgs serverArgs = new ServerArgs(in);
            System.out.println("RUNNING ELASTICSEARCH");
            System.out.println(serverArgs);
            elasticsearch.init(
                serverArgs.daemonize(),
                serverArgs.pidFile(),
                serverArgs.quiet(),
                new Environment(serverArgs.nodeSettings(), serverArgs.configDir()),
                serverArgs.keystorePassword()
            );
        } catch (NodeValidationException e) {
            exitCode = ExitCodes.CONFIG;
            err.print(USER_EXCEPTION_MARKER);
            err.println(e.getMessage());
        } catch (UserException e) {
            exitCode = e.exitCode;
            err.print(USER_EXCEPTION_MARKER);
            err.println(e.getMessage());
        } catch (Exception e) {
            exitCode = 1; // mimic JDK exit code on exception
            if (System.getProperty("es.logs.base_path") != null) {
                // this is a horrible hack to see if logging has been initialized
                // we need to find a better way!
                Logger logger = LogManager.getLogger(Elasticsearch.class);
                logger.error("fatal exception while booting Elasticsearch", e);
            }
            e.printStackTrace(err);
        }
        if (exitCode != ExitCodes.OK) {
            printLogsSuggestion(err);
            err.flush();
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

    void init(final boolean daemonize, final Path pidFile, final boolean quiet, Environment initialEnv, SecureString keystorePassword)
        throws NodeValidationException, UserException {
        try {
            Bootstrap.init(daemonize == false, pidFile, quiet, initialEnv, keystorePassword);
        } catch (BootstrapException | RuntimeException e) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupException(e);
        }
    }
}
