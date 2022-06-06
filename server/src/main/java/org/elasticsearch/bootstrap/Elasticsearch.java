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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeValidationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
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
        PrintStream out = getStdout();
        PrintStream err = getStderr();
        try {
            final var in = new InputStreamStreamInput(System.in);
            final ServerArgs serverArgs = new ServerArgs(in);
            initPidFile(serverArgs.pidFile());
            elasticsearch.init(
                serverArgs.daemonize(),
                serverArgs.quiet(),
                new Environment(serverArgs.nodeSettings(), serverArgs.configDir()),
                serverArgs.keystorePassword(),
                serverArgs.pidFile()
            );

            err.println(BootstrapInfo.SERVER_READY_MARKER);
            if (serverArgs.daemonize()) {
                out.close();
                err.close();
            } else {
                startCliMonitorThread(System.in);
            }

        } catch (NodeValidationException e) {
            exitWithUserException(err, ExitCodes.CONFIG, e);
        } catch (UserException e) {
            exitWithUserException(err, e.exitCode, e);
        } catch (Exception e) {
            exitWithUnknownException(err, e);
        }
    }

    private static void exitWithUserException(PrintStream err, int exitCode, Exception e) {
        err.print(USER_EXCEPTION_MARKER);
        err.println(e.getMessage());
        gracefullyExit(err, exitCode);
    }

    private static void exitWithUnknownException(PrintStream err, Exception e) {
        if (System.getProperty("es.logs.base_path") != null) {
            // this is a horrible hack to see if logging has been initialized
            // we need to find a better way!
            Logger logger = LogManager.getLogger(Elasticsearch.class);
            logger.error("fatal exception while booting Elasticsearch", e);
        }
        e.printStackTrace(err);
        gracefullyExit(err, 1); // mimic JDK exit code on exception
    }

    private static void gracefullyExit(PrintStream err, int exitCode) {
        printLogsSuggestion(err);
        err.flush();
        exit(exitCode);
    }

    @SuppressForbidden(reason = "grab stderr for communication with server-cli")
    private static PrintStream getStderr() {
        return System.err;
    }

    // TODO: remove this, just for debugging
    @SuppressForbidden(reason = "grab stdout for communication with server-cli")
    private static PrintStream getStdout() {
        return System.out;
    }

    @SuppressForbidden(reason = "main exit path")
    private static void exit(int exitCode) {
        System.exit(exitCode);
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

    /**
     * Starts a thread that monitors stdin for a shutdown signal.
     *
     * If the shutdown signal is received, Elasticsearch exits with status code 0.
     * If the pipe is broken, Elasticsearch exits with status code 1.
     *
     * @param stdin Standard input for this process
     */
    private static void startCliMonitorThread(InputStream stdin) {
        new Thread(() -> {
            int msg = -1;
            try {
                msg = stdin.read();
            } catch (IOException e) {
                // ignore, whether we cleanly got end of stream (-1) or an error, we will shut down below
            } finally {
                if (msg == BootstrapInfo.SERVER_SHUTDOWN_MARKER) {
                    exit(0);
                } else {
                    // parent process died or there was an error reading from it
                    exit(1);
                }
            }
        }).start();
    }

    /**
     * Writes the current process id into the given pidfile, if not null. The pidfile is cleaned up on system exit.
     *
     * @param pidFile A path to a file, or null of no pidfile should be written
     */
    private static void initPidFile(Path pidFile) throws IOException {
        if (pidFile == null) {
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.deleteIfExists(pidFile);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to delete pid file " + pidFile, e);
            }
        }, "elasticsearch[pidfile-cleanup]"));

        // It has to be an absolute path, otherwise pidFile.getParent() will return null
        assert pidFile.isAbsolute();

        if (Files.exists(pidFile.getParent()) == false) {
            Files.createDirectories(pidFile.getParent());
        }

        Files.writeString(pidFile, Long.toString(ProcessHandle.current().pid()));
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

    void init(final boolean daemonize, final boolean quiet, Environment initialEnv, SecureString keystorePassword, Path pidFile)
        throws NodeValidationException, UserException {
        try {
            Bootstrap.init(daemonize == false, quiet, initialEnv, keystorePassword, pidFile);
        } catch (BootstrapException | RuntimeException e) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupException(e);
        }
    }
}
