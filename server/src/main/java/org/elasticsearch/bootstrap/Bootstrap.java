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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeValidationException;

import java.io.PrintStream;

/**
 * A container for transient state during bootstrap of the Elasticsearch process.
 */
class Bootstrap {
    // original stdout stream
    private final PrintStream out;

    // original stderr stream
    private final PrintStream err;

    // arguments from the CLI process
    private final ServerArgs args;

    // controller for spawning component subprocesses
    private final Spawner spawner = new Spawner();

    // the loaded keystore, not valid until after phase 2 of initialization
    private final SetOnce<SecureSettings> secureSettings = new SetOnce<>();

    // the loaded settings for the node, not valid until after phase 2 of initialization
    private final SetOnce<Environment> nodeEnv = new SetOnce<>();

    Bootstrap(PrintStream out, PrintStream err, ServerArgs args) {
        this.out = out;
        this.err = err;
        this.args = args;
    }

    ServerArgs args() {
        return args;
    }

    Spawner spawner() {
        return spawner;
    }

    void setSecureSettings(SecureSettings secureSettings) {
        this.secureSettings.set(secureSettings);
    }

    SecureSettings secureSettings() {
        return secureSettings.get();
    }

    void setEnvironment(Environment environment) {
        this.nodeEnv.set(environment);
    }

    Environment environment() {
        return nodeEnv.get();
    }

    void exitWithNodeValidationException(NodeValidationException e) {
        Logger logger = LogManager.getLogger(Elasticsearch.class);
        logger.error("node validation exception\n{}", e.getMessage());
        gracefullyExit(ExitCodes.CONFIG);
    }

    void exitWithUnknownException(Throwable e) {
        Logger logger = LogManager.getLogger(Elasticsearch.class);
        logger.error("fatal exception while booting Elasticsearch", e);
        gracefullyExit(1); // mimic JDK exit code on exception
    }

    private void gracefullyExit(int exitCode) {
        printLogsSuggestion();
        err.flush();
        exit(exitCode);
    }

    @SuppressForbidden(reason = "main exit path")
    static void exit(int exitCode) {
        System.exit(exitCode);
    }

    /**
     * Prints a message directing the user to look at the logs. A message is only printed if
     * logging has been configured.
     */
    private void printLogsSuggestion() {
        final String basePath = System.getProperty("es.logs.base_path");
        assert basePath != null : "logging wasn't initialized";
        err.println(
            "ERROR: Elasticsearch did not exit normally - check the logs at "
                + basePath
                + System.getProperty("file.separator")
                + System.getProperty("es.logs.cluster_name")
                + ".log"
        );
    }

    void sendCliMarker(char marker) {
        err.println(marker);
        err.flush();
    }

    void closeStreams() {
        out.close();
        err.close();
    }
}
