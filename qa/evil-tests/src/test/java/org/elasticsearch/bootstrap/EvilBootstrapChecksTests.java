/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
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

public class EvilBootstrapChecksTests extends AbstractBootstrapCheckTestCase {

    private String esEnforceBootstrapChecks = System.getProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        setEsEnforceBootstrapChecks(esEnforceBootstrapChecks);
        super.tearDown();
    }

    public void testEnforceBootstrapChecks() throws NodeValidationException {
        setEsEnforceBootstrapChecks("true");
        final List<BootstrapCheck> checks = Collections.singletonList(new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                return BootstrapCheck.BootstrapCheckResult.failure("error");
            }

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
            }
        });

        final Logger logger = mock(Logger.class);

        final NodeValidationException e = expectThrows(
            NodeValidationException.class,
            () -> BootstrapChecks.check(emptyContext, false, checks, logger)
        );
        final Matcher<String> allOf = allOf(containsString("bootstrap checks failed"), containsString("error"));
        assertThat(e, hasToString(allOf));
        verify(logger).info("explicitly enforcing bootstrap checks");
        verifyNoMoreInteractions(logger);
    }

    public void testNonEnforcedBootstrapChecks() throws NodeValidationException {
        setEsEnforceBootstrapChecks(null);
        final Logger logger = mock(Logger.class);
        // nothing should happen
        BootstrapChecks.check(emptyContext, false, emptyList(), logger);
        verifyNoMoreInteractions(logger);
    }

    public void testInvalidValue() {
        final String value = randomAlphaOfLength(8);
        setEsEnforceBootstrapChecks(value);
        final boolean enforceLimits = randomBoolean();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> BootstrapChecks.check(emptyContext, enforceLimits, emptyList())
        );
        final Matcher<String> matcher = containsString("[es.enforce.bootstrap.checks] must be [true] but was [" + value + "]");
        assertThat(e, hasToString(matcher));
    }

    @SuppressForbidden(reason = "set or clear system property es.enforce.bootstrap.checks")
    public void setEsEnforceBootstrapChecks(final String value) {
        if (value == null) {
            System.clearProperty(ES_ENFORCE_BOOTSTRAP_CHECKS);
        } else {
            System.setProperty(ES_ENFORCE_BOOTSTRAP_CHECKS, value);
        }
    }

}
