/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.junit.listeners;

import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.Locale;
import java.util.TimeZone;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_ITERATIONS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_PREFIX;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;
import static org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase.REST_TESTS_BLACKLIST;
import static org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase.REST_TESTS_SUITE;

/**
 * A {@link RunListener} that emits a command you can use to re-run a failing test with the failing random seed to
 * {@link System#err}.
 */
public class ReproduceInfoPrinter extends RunListener {

    protected final Logger logger = Loggers.getLogger(ESTestCase.class);

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
        // TODO: enforce (intellij still runs the runner?) or use default "test" but that won't work for integ
        b.append(task);

        GradleMessageBuilder gradleMessageBuilder = new GradleMessageBuilder(b);
        gradleMessageBuilder.appendAllOpts(failure.getDescription());

        // Client yaml suite tests are a special case as they allow for additional parameters
        if (ESClientYamlSuiteTestCase.class.isAssignableFrom(failure.getDescription().getTestClass())) {
            gradleMessageBuilder.appendClientYamlSuiteProperties();
        }

        System.err.println(b.toString());
    }

    protected static class GradleMessageBuilder extends ReproduceErrorMessageBuilder {

        public GradleMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {
            super.appendAllOpts(description);

            if (description.getMethodName() != null) {
                //prints out the raw method description instead of methodName(description) which filters out the parameters
                super.appendOpt(SYSPROP_TESTMETHOD(), "\"" + description.getMethodName() + "\"");
            }

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
            if (sysPropName.equals(SYSPROP_TESTMETHOD())) {
                //don't print out the test method, we print it ourselves in appendAllOpts
                //without filtering out the parameters (needed for REST tests)
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

        public ReproduceErrorMessageBuilder appendESProperties() {
            appendProperties("tests.es.logger.level");
            if (inVerifyPhase()) {
                // these properties only make sense for integration tests
                appendProperties(ESIntegTestCase.TESTS_ENABLE_MOCK_MODULES);
            }
            appendProperties("tests.assertion.disabled", "tests.security.manager", "tests.nightly", "tests.jvms",
                             "tests.client.ratio", "tests.heap.size", "tests.bwc", "tests.bwc.version", "build.snapshot");
            if (System.getProperty("tests.jvm.argline") != null && !System.getProperty("tests.jvm.argline").isEmpty()) {
                appendOpt("tests.jvm.argline", "\"" + System.getProperty("tests.jvm.argline") + "\"");
            }
            appendOpt("tests.locale", Locale.getDefault().toLanguageTag());
            appendOpt("tests.timezone", TimeZone.getDefault().getID());
            return this;
        }

        public ReproduceErrorMessageBuilder appendClientYamlSuiteProperties() {
            return appendProperties(REST_TESTS_SUITE, REST_TESTS_BLACKLIST);
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
