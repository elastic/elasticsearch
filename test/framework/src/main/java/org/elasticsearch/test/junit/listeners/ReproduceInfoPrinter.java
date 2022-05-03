/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.junit.listeners;

import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.Locale;
import java.util.TimeZone;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_ITERATIONS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_PREFIX;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;

/**
 * A {@link RunListener} that emits a command you can use to re-run a failing test with the failing random seed to
 * {@link System#err}.
 */
public class ReproduceInfoPrinter extends RunListener {

    protected final Logger logger = LogManager.getLogger(ESTestCase.class);

    @Override
    public void testStarted(Description description) throws Exception {
        logger.trace("Test {} started", description.getDisplayName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        logger.trace("Test {} finished", description.getDisplayName());
    }

    /**
     * Are we in the integ test phase?
     */
    static boolean inVerifyPhase() {
        return Boolean.parseBoolean(System.getProperty("tests.verify.phase"));
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        // Ignore assumptions.
        if (failure.getException() instanceof AssumptionViolatedException) {
            return;
        }

        final String gradlew = Constants.WINDOWS ? "gradlew" : "./gradlew";
        final StringBuilder b = new StringBuilder("REPRODUCE WITH: " + gradlew + " ");
        String task = System.getProperty("tests.task");
        boolean isBwcTest = Boolean.parseBoolean(System.getProperty("tests.bwc", "false"));

        // append Gradle test runner test filter string
        b.append("'" + task + "'");
        if (isBwcTest) {
            // Use "legacy" method for bwc tests so that it applies globally to all upstream bwc test tasks
            b.append(" -Dtests.class=\"");
        } else {
            b.append(" --tests \"");
        }
        b.append(failure.getDescription().getClassName());

        final String methodName = failure.getDescription().getMethodName();
        if (methodName != null) {
            // fallback to system property filter when tests contain "."
            if (methodName.contains(".") || isBwcTest) {
                b.append("\" -Dtests.method=\"");
                b.append(methodName);
            } else {
                b.append(".");
                b.append(methodName);
            }
        }
        b.append("\"");
        GradleMessageBuilder gradleMessageBuilder = new GradleMessageBuilder(b);
        gradleMessageBuilder.appendAllOpts(failure.getDescription());

        printToErr(b.toString());
    }

    @SuppressForbidden(reason = "printing repro info")
    private static void printToErr(String s) {
        System.err.println(s);
    }

    protected static class GradleMessageBuilder extends ReproduceErrorMessageBuilder {

        public GradleMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {
            super.appendAllOpts(description);

            return appendESProperties();
        }

        @Override
        public ReproduceErrorMessageBuilder appendEnvironmentSettings() {
            // we handle our own environment settings
            return this;
        }

        /**
         * Append a single VM option.
         */
        @Override
        public ReproduceErrorMessageBuilder appendOpt(String sysPropName, String value) {
            if (sysPropName.equals(SYSPROP_ITERATIONS())) { // we don't want the iters to be in there!
                return this;
            }
            if (sysPropName.equals(SYSPROP_TESTCLASS())) {
                // don't print out the test class, we print it ourselves in appendAllOpts
                // without filtering out the parameters (needed for REST tests)
                return this;
            }
            if (sysPropName.equals(SYSPROP_TESTMETHOD())) {
                // don't print out the test method, we print it ourselves in appendAllOpts
                // without filtering out the parameters (needed for REST tests)
                return this;
            }
            if (sysPropName.equals(SYSPROP_PREFIX())) {
                // we always use the default prefix
                return this;
            }
            if (Strings.hasLength(value)) {
                return super.appendOpt(sysPropName, value);
            }
            return this;
        }

        private ReproduceErrorMessageBuilder appendESProperties() {
            appendProperties("tests.es.logger.level");
            if (inVerifyPhase()) {
                // these properties only make sense for integration tests
                appendProperties(ESIntegTestCase.TESTS_ENABLE_MOCK_MODULES);
            }
            appendProperties(
                "tests.assertion.disabled",
                "tests.nightly",
                "tests.jvms",
                "tests.client.ratio",
                "tests.heap.size",
                "tests.bwc",
                "tests.bwc.version",
                "build.snapshot"
            );
            if (System.getProperty("tests.jvm.argline") != null && System.getProperty("tests.jvm.argline").isEmpty() == false) {
                appendOpt("tests.jvm.argline", "\"" + System.getProperty("tests.jvm.argline") + "\"");
            }
            appendOpt("tests.locale", Locale.getDefault().toLanguageTag());
            appendOpt("tests.timezone", TimeZone.getDefault().getID());
            appendOpt("tests.distribution", System.getProperty("tests.distribution"));
            appendOpt("runtime.java", Integer.toString(Runtime.version().feature()));
            appendOpt("license.key", System.getProperty("licence.key"));
            appendOpt(ESTestCase.FIPS_SYSPROP, System.getProperty(ESTestCase.FIPS_SYSPROP));
            return this;
        }

        protected ReproduceErrorMessageBuilder appendProperties(String... properties) {
            for (String sysPropName : properties) {
                if (Strings.hasLength(System.getProperty(sysPropName))) {
                    appendOpt(sysPropName, System.getProperty(sysPropName));
                }
            }
            return this;
        }

    }
}
