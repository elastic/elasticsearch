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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;
import com.carrotsearch.randomizedtesting.TraceFormatting;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.rest.ElasticsearchRestTests;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_ITERATIONS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.TESTS_CLUSTER;
import static org.elasticsearch.test.rest.ElasticsearchRestTests.REST_TESTS_BLACKLIST;
import static org.elasticsearch.test.rest.ElasticsearchRestTests.REST_TESTS_SPEC;
import static org.elasticsearch.test.rest.ElasticsearchRestTests.REST_TESTS_SUITE;


/**
 * A {@link RunListener} that emits to {@link System#err} a string with command
 * line parameters allowing quick test re-run under MVN command line.
 */
public class ReproduceInfoPrinter extends RunListener {

    protected final ESLogger logger = Loggers.getLogger(ElasticsearchTestCase.class);

    @Override
    public void testStarted(Description description) throws Exception {
        logger.info("Test {} started", description.getDisplayName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        logger.info("Test {} finished", description.getDisplayName());
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        // Ignore assumptions.
        if (failure.getException() instanceof AssumptionViolatedException) {
            return;
        }

        final Description d = failure.getDescription();
        final StringBuilder b = new StringBuilder();
        b.append("FAILURE  : ").append(d.getDisplayName()).append("\n");
        b.append("REPRODUCE WITH  : mvn clean test");
        MavenMessageBuilder mavenMessageBuilder = new MavenMessageBuilder(b);
        mavenMessageBuilder.appendAllOpts(failure.getDescription());

        //ElasticsearchRestTests is a special case as it allows for additional parameters
        if (ElasticsearchRestTests.class.isAssignableFrom(failure.getDescription().getTestClass())) {
            mavenMessageBuilder.appendRestTestsProperties();
        }

        b.append("\n");
        b.append("Throwable:\n");
        if (failure.getException() != null) {
            traces().formatThrowable(b, failure.getException());
        }

        logger.error(b.toString());
    }

    protected TraceFormatting traces() {
        TraceFormatting traces = new TraceFormatting();
        try {
            traces = RandomizedContext.current().getRunner().getTraceFormatting();
        } catch (IllegalStateException e) {
            // Ignore if no context.
        }
        return traces;
    }

    protected static class MavenMessageBuilder extends ReproduceErrorMessageBuilder {

        public MavenMessageBuilder(StringBuilder b) {
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
            if (Strings.hasLength(value)) {
                return super.appendOpt(sysPropName, value);
            }
            return this;
        }

        public ReproduceErrorMessageBuilder appendESProperties() {
            appendProperties("es.logger.level", "es.node.mode", "es.node.local", TESTS_CLUSTER, InternalTestCluster.TESTS_ENABLE_MOCK_MODULES,
                    "tests.assertion.disabled", "tests.security.manager", "tests.nightly", "tests.jvms", "tests.client.ratio", "tests.heap.size",
                    "tests.bwc", "tests.bwc.version");
            if (System.getProperty("tests.jvm.argline") != null && !System.getProperty("tests.jvm.argline").isEmpty()) {
                appendOpt("tests.jvm.argline", "\"" + System.getProperty("tests.jvm.argline") + "\"");
            }
            appendOpt(AbstractRandomizedTest.SYSPROP_PROCESSORS, Integer.toString(AbstractRandomizedTest.TESTS_PROCESSORS));
            return this;
        }

        public ReproduceErrorMessageBuilder appendRestTestsProperties() {
            return appendProperties(REST_TESTS_SUITE, REST_TESTS_SPEC, REST_TESTS_BLACKLIST);
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
