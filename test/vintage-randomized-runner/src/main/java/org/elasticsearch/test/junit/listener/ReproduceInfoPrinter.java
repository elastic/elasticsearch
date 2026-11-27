/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.junit.listener;

import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.junit.runner.IterationSeeds;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.Locale;
import java.util.TimeZone;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_ITERATIONS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_PREFIX;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_RANDOM_SEED;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;

/**
 * A {@link RunListener} that emits a command you can use to re-run a failing test with the failing random seed to
 * {@link System#err}.
 * <p>
 * This listener is registered with {@code @Listeners}, which is handled by the {@code RandomizedRunner} that
 * {@link org.elasticsearch.test.junit.runner.ESRandomizedRunner} wraps, not by the wrapper. It therefore sees the descriptions as
 * {@code RandomizedRunner} creates them, where with {@code tests.iters} the name of a test has the seed of its iteration
 * ({@code testFoo {seed=[X:Y]}}).
 */
public class ReproduceInfoPrinter extends RunListener {

    private static final boolean IS_WINDOWS = System.getProperty("os.name", "").startsWith("Windows");
    private static final String FIPS_SYSPROP = "tests.fips.enabled";
    private static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";

    /**
     * Are we in the integ test phase?
     */
    static boolean inVerifyPhase() {
        return parseBoolean(System.getProperty("tests.verify.phase", "false"));
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        // Ignore assumptions.
        if (failure.getException() instanceof AssumptionViolatedException) {
            return;
        }
        printToErr(reproduceMessage(failure));
    }

    // visible for testing
    static String reproduceMessage(Failure failure) {
        final String gradlew = IS_WINDOWS ? "gradlew" : "./gradlew";
        final StringBuilder b = new StringBuilder("REPRODUCE WITH: " + gradlew + " ");
        String task = System.getProperty("tests.task");
        boolean isBwcTest = parseBoolean(System.getProperty("tests.bwc", "false"))
            || System.getProperty("tests.bwc.main.version") != null
            || System.getProperty("tests.bwc.refspec.main") != null;

        // append Gradle test runner test filter string
        b.append("\"" + task + "\"");
        if (isBwcTest) {
            // Use "legacy" method for bwc tests so that it applies globally to all upstream bwc test tasks
            b.append(" -Dtests.class=\"");
        } else {
            b.append(" --tests \"");
        }
        b.append(failure.getDescription().getClassName());

        final String methodName = IterationSeeds.withoutSeed(failure.getDescription().getMethodName());
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

        if (isRestApiCompatibilityTest()) {
            b.append(System.lineSeparator());
            b.append(
                "This is a Rest Api Compatibility Test. "
                    + "See the developers guide for details how to troubleshoot - "
                    + "https://github.com/elastic/elasticsearch/blob/main/REST_API_COMPATIBILITY.md"
            );
        }
        return b.toString();
    }

    private static boolean isRestApiCompatibilityTest() {
        return parseBoolean(System.getProperty("tests.restCompat", "false"));
    }

    @SuppressForbidden(reason = "printing repro info")
    private static void printToErr(String s) {
        System.err.println(s);
    }

    protected static class GradleMessageBuilder extends ReproduceErrorMessageBuilder {

        @Nullable
        private String iterationSeed;

        public GradleMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {
            iterationSeed = IterationSeeds.seedOf(description.getMethodName());
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
            if (iterationSeed != null && sysPropName.equals(SYSPROP_RANDOM_SEED())) {
                // the seed of the iteration that failed (classSeed:methodSeed), the name of the test does not tell iterations apart
                return super.appendOpt(sysPropName, iterationSeed);
            }
            if (hasLength(value)) {
                return super.appendOpt(sysPropName, value);
            }
            return this;
        }

        private ReproduceErrorMessageBuilder appendESProperties() {
            appendProperties("tests.es.logger.level");
            if (inVerifyPhase()) {
                // these properties only make sense for integration tests
                appendProperties(TESTS_ENABLE_MOCK_MODULES);
            }
            appendProperties(
                "tests.assertion.disabled",
                "tests.nightly",
                "tests.jvms",
                "tests.client.ratio",
                "tests.heap.size",
                "tests.bwc",
                "tests.bwc.version",
                "build.snapshot",
                "tests.configure_test_clusters_with_one_processor",
                "tests.bwc.main.version",
                "tests.bwc.refspec.main",
                "tests.vectorsize"
            );
            if (System.getProperty("tests.jvm.argline") != null && System.getProperty("tests.jvm.argline").isEmpty() == false) {
                appendOpt("tests.jvm.argline", "\"" + System.getProperty("tests.jvm.argline") + "\"");
            }
            if (parseBoolean(System.getProperty("build.snapshot", "true")) == false) {
                appendOpt("license.key", "x-pack/license-tools/src/test/resources/public.key");
            }
            appendOpt("tests.locale", Locale.getDefault().toLanguageTag());
            appendOpt("tests.timezone", TimeZone.getDefault().getID());
            appendOpt("tests.distribution", System.getProperty("tests.distribution"));
            if (Runtime.version().build().isPresent()
                && ("ea".equalsIgnoreCase(Runtime.version().pre().orElse(""))
                    || ("rc".equalsIgnoreCase(Runtime.version().pre().orElse(""))))) {
                appendOpt("runtime.java", Runtime.version().feature() + "-pre");
                appendOpt("runtime.java.build", Integer.toString(Runtime.version().build().get()));
            } else {
                appendOpt("runtime.java", Integer.toString(Runtime.version().feature()));
            }
            appendOpt(FIPS_SYSPROP, System.getProperty(FIPS_SYSPROP));
            return this;
        }

        private void appendProperties(String... properties) {
            for (String sysPropName : properties) {
                if (hasLength(System.getProperty(sysPropName))) {
                    appendOpt(sysPropName, System.getProperty(sysPropName));
                }
            }
        }
    }

    private static boolean hasLength(@Nullable String s) {
        return s != null && s.isEmpty() == false;
    }

    private static boolean parseBoolean(String value) {
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new AssertionError("not a boolean: " + value);
    }
}
