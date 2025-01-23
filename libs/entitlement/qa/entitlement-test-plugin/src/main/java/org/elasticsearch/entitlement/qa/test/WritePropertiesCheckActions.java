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

@SuppressForbidden(reason = "testing entitlements")
class WritePropertiesCheckActions {
    private WritePropertiesCheckActions() {}

    static void setSystemProperty() {
        System.setProperty("es.entitlements.checkSetSystemProperty", "true");
        try {
            System.clearProperty("es.entitlements.checkSetSystemProperty");
        } catch (RuntimeException e) {
            // ignore for this test case
        }

    }

    static void clearSystemProperty() {
        System.clearProperty("es.entitlements.checkClearSystemProperty");
    }

    static void setSystemProperties() {
        System.setProperties(System.getProperties()); // no side effect in case if allowed (but shouldn't)
    }
}
