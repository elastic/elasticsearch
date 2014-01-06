/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.junit;

import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;
import com.carrotsearch.randomizedtesting.StandaloneRandomizedContext;
import com.carrotsearch.randomizedtesting.TraceFormatting;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.test.rest.ElasticsearchRestTests;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;

import java.util.Arrays;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_RANDOM_SEED;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static org.elasticsearch.test.rest.junit.RestTestSuiteRunner.*;

/**
 * A {@link org.junit.runner.notification.RunListener} that emits to {@link System#err} a string with command
 * line parameters allowing quick REST test re-run under MVN command line.
 */
class RestReproduceInfoPrinter extends ReproduceInfoPrinter {

    protected static final ESLogger logger = Loggers.getLogger(RestReproduceInfoPrinter.class);

    @Override
    protected boolean mustAppendClusterSeed(Failure failure) {
        return isTestCluster();
    }

    private static boolean isTestCluster() {
        return runMode() == RunMode.TEST_CLUSTER;
    }

    @Override
    protected TraceFormatting traces() {
        return new TraceFormatting(
                Arrays.asList(
                    "org.junit.",
                    "junit.framework.",
                    "sun.",
                    "java.lang.reflect.",
                    "com.carrotsearch.randomizedtesting.",
                    "org.elasticsearch.test.rest.junit."
                ));
    }

    @Override
    protected ReproduceErrorMessageBuilder reproduceErrorMessageBuilder(StringBuilder b) {
        return new MavenMessageBuilder(b);
    }

    private static class MavenMessageBuilder extends ReproduceInfoPrinter.MavenMessageBuilder {

        public MavenMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {

            try {
                appendOpt(SYSPROP_RANDOM_SEED(), StandaloneRandomizedContext.getSeedAsString());
            } catch (IllegalStateException e) {
                logger.warn("No context available when dumping reproduce options?");
            }

            //we know that ElasticsearchRestTests is the only one that runs with RestTestSuiteRunner
            appendOpt(SYSPROP_TESTCLASS(), ElasticsearchRestTests.class.getName());

            if (description.getClassName() != null) {
                appendOpt(REST_TESTS_SUITE, description.getClassName());
            }

            appendRunnerProperties();
            appendEnvironmentSettings();

            appendProperties("es.logger.level");

            if (isTestCluster()) {
                appendProperties("es.node.mode", "es.node.local");
            }

            appendRestTestsProperties();

            return this;
        }

        public ReproduceErrorMessageBuilder appendRestTestsProperties() {
            return appendProperties(REST_TESTS_MODE, REST_TESTS_SPEC);
        }
    }
}
