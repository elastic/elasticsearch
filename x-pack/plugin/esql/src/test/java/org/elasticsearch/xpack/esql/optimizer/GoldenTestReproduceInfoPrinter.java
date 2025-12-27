/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/** Adds -Dgolden.bulldoze to the reproduction line for golden test failures. */
public class GoldenTestReproduceInfoPrinter extends RunListener {
    private final ReproduceInfoPrinter delegate = new ReproduceInfoPrinter();

    @Override
    public void testFailure(Failure failure) throws Exception {
        if (failure.getException() instanceof AssumptionViolatedException) {
            return;
        }
        if (isGoldenTest(failure)) {
            printToErr(captureDelegate(failure).replace("REPRODUCE WITH:", "BULLDOZE WITH:") + " -Dgolden.bulldoze");
        } else {
            delegate.testFailure(failure);
        }
    }

    private String captureDelegate(Failure failure) throws Exception {
        PrintStream originalErr = System.err;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setErr(new PrintStream(baos, true, StandardCharsets.UTF_8));
        try {
            delegate.testFailure(failure);
        } finally {
            System.setErr(originalErr);
        }
        return baos.toString(StandardCharsets.UTF_8).trim();
    }

    private static boolean isGoldenTest(Failure failure) {
        try {
            return GoldenTestCase.class.isAssignableFrom(Class.forName(failure.getDescription().getClassName()));
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @SuppressForbidden(reason = "printing repro info")
    private static void printToErr(String s) {
        System.err.println(s);
    }
}
