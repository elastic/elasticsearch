/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap.ATTACH_TIMEOUT_MILLIS;
import static org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap.ATTACH_TIMEOUT_PROPERTY;

@SuppressForbidden(reason = "Tests need to read/write the attach timeout system property")
public class EntitlementBootstrapTests extends ESTestCase {

    private String previousValue;

    @Before
    public void capturePreviousValue() {
        previousValue = System.getProperty(ATTACH_TIMEOUT_PROPERTY);
        System.clearProperty(ATTACH_TIMEOUT_PROPERTY);
    }

    @After
    public void restorePreviousValue() {
        if (previousValue == null) {
            System.clearProperty(ATTACH_TIMEOUT_PROPERTY);
        } else {
            System.setProperty(ATTACH_TIMEOUT_PROPERTY, previousValue);
        }
    }

    public void testSetAttachTimeoutWhenUnset() {
        EntitlementBootstrap.setAttachTimeoutIfUnset();
        assertEquals(ATTACH_TIMEOUT_MILLIS, System.getProperty(ATTACH_TIMEOUT_PROPERTY));
    }

    public void testSetAttachTimeoutDoesNotOverrideExistingValue() {
        System.setProperty(ATTACH_TIMEOUT_PROPERTY, "5000");
        EntitlementBootstrap.setAttachTimeoutIfUnset();
        assertEquals("5000", System.getProperty(ATTACH_TIMEOUT_PROPERTY));
    }
}
