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
 *
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.bootstrap.BootstrapChecks.ES_ENFORCE_BOOTSTRAP_CHECKS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class EvilBootstrapChecksTests extends ESTestCase {

    private String esEnforceBootstrapChecks = System.getProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @SuppressForbidden(reason = "set or clear system property es.enforce.bootstrap.checks")
    @Override
    @After
    public void tearDown() throws Exception {
        if (esEnforceBootstrapChecks == null) {
            System.clearProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);
        } else {
            System.setProperty(ES_ENFORCE_BOOTSTRAP_CHECKS, esEnforceBootstrapChecks);
        }
        super.tearDown();
    }

    @SuppressForbidden(reason = "set system property es.enforce.bootstrap.checks")
    public void testEnforceBootstrapChecks() throws NodeValidationException {
        System.setProperty(ES_ENFORCE_BOOTSTRAP_CHECKS, "true");
        final List<BootstrapCheck> checks = Collections.singletonList(
                new BootstrapCheck() {
                    @Override
                    public boolean check() {
                        return true;
                    }

                    @Override
                    public String errorMessage() {
                        return "error";
                    }
                }
        );
        final Logger logger = mock(Logger.class);

        final NodeValidationException e = expectThrows(
                NodeValidationException.class,
                () -> BootstrapChecks.check(false, checks, logger));
        final Matcher<String> allOf =
                allOf(containsString("bootstrap checks failed"), containsString("error"));
        assertThat(e, hasToString(allOf));
        verify(logger).info("explicitly enforcing bootstrap checks");
        verifyNoMoreInteractions(logger);
    }

    @SuppressForbidden(reason = "clear system property es.enforce.bootstrap.checks")
    public void testNonEnforcedBootstrapChecks() throws NodeValidationException {
        System.clearProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);
        final Logger logger = mock(Logger.class);
        // nothing should happen
        BootstrapChecks.check(false, emptyList(), logger);
        verifyNoMoreInteractions(logger);
    }

    @SuppressForbidden(reason = "set system property es.enforce.bootstrap.checks")
    public void testInvalidValue() {
        final String value = randomAsciiOfLength(8);
        System.setProperty(ES_ENFORCE_BOOTSTRAP_CHECKS, value);
        final boolean enforceLimits = randomBoolean();
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> BootstrapChecks.check(enforceLimits, emptyList(), "testInvalidValue"));
        final Matcher<String> matcher = containsString(
                "[es.enforce.bootstrap.checks] must be [true] but was [" + value + "]");
        assertThat(e, hasToString(matcher));
    }

}
