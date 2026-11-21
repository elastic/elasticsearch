/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;

@SuppressWarnings({ "unused" /* called via reflection */ })
class OperatingSystemActions {

    /** A non-empty command, since {@link Runtime#exec} rejects empty commands before any process is started. */
    private static final String COMMAND = "entitlement-test-nonexistent-command";

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void processBuilder_start() throws IOException {
        new ProcessBuilder(COMMAND).start();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void processBuilder_startPipeline() throws IOException {
        ProcessBuilder.startPipeline(List.of(new ProcessBuilder(COMMAND)));
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void runtime_execCommand() throws IOException {
        Runtime.getRuntime().exec(COMMAND);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void runtime_execCommandWithEnv() throws IOException {
        Runtime.getRuntime().exec(COMMAND, new String[0]);
    }

    @SuppressForbidden(reason = "Runtime.exec only accepts a java.io.File for the working directory")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void runtime_execCommandWithEnvAndDir() throws IOException {
        Runtime.getRuntime().exec(COMMAND, new String[0], new File("."));
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void runtime_execCommandArray() throws IOException {
        Runtime.getRuntime().exec(new String[] { COMMAND });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void runtime_execCommandArrayWithEnv() throws IOException {
        Runtime.getRuntime().exec(new String[] { COMMAND }, new String[0]);
    }

    @SuppressForbidden(reason = "Runtime.exec only accepts a java.io.File for the working directory")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void runtime_execCommandArrayWithEnvAndDir() throws IOException {
        Runtime.getRuntime().exec(new String[] { COMMAND }, new String[0], new File("."));
    }

    private OperatingSystemActions() {}
}
