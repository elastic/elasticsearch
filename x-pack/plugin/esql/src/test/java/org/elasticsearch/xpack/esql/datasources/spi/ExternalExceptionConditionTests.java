/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;

/**
 * Pins the message every {@link Condition} renders, with and without an object name, detail code and remedy. These
 * strings are what users see and what tests and clients match on, so a change to one has to be deliberate.
 */
public class ExternalExceptionConditionTests extends ESTestCase {

    public void testObjectConditionsNameTheObjectOrFallBack() {
        assertRendered(Condition.STORE_UNAVAILABLE, "External store unavailable reading [x.csv]", "External store unavailable");
        assertRendered(Condition.STORE_THROTTLED, "External store throttled reading [x.csv]", "External store throttled");
        assertRendered(
            Condition.OBJECT_CHANGED,
            "External data object [x.csv] was modified during read",
            "External data object was modified during read"
        );
        assertRendered(Condition.ACCESS_DENIED, "Access denied reading [x.csv]", "Access denied reading external data");
        assertRendered(Condition.OBJECT_NOT_FOUND, "External data object not found: [x.csv]", "External data object not found");
        assertRendered(Condition.MALFORMED_DATA, "Malformed data in [x.csv]", "Malformed external data");
        assertRendered(Condition.METADATA_UNAVAILABLE, "Failed to get metadata for [x.csv]", "Failed to get external data metadata");
        assertRendered(Condition.LISTING_FAILED, "Failed to list external data objects", "Failed to list external data objects");
    }

    public void testDetailCodeAndRemedyAreAppended() {
        assertEquals(
            "Access denied reading [x.csv] (HTTP 403 AccessDenied). Check the credentials.",
            Condition.ACCESS_DENIED.render("x.csv", "HTTP 403 AccessDenied", "Check the credentials.")
        );
        assertEquals("External store throttled (HTTP 503)", Condition.STORE_THROTTLED.render("", "HTTP 503", ""));
        assertEquals("Failed to list external data objects. Retry.", Condition.LISTING_FAILED.render("x.csv", "", "Retry."));
    }

    public void testSpecialConditions() {
        assertEquals(
            "Session credentials expired or invalid. Refresh the data source credentials and re-run the query.",
            Condition.CREDENTIALS_EXPIRED.render("x.csv", "", "")
        );
        assertEquals(
            "Session credentials expired or invalid. Rotate the token. (HTTP 400 ExpiredToken)",
            Condition.CREDENTIALS_EXPIRED.render("", "HTTP 400 ExpiredToken", "Rotate the token.")
        );
        assertEquals(
            "Request rejected due to clock skew: the host clock differs too much from the storage service. "
                + "Check that the host clock is NTP-synchronized.",
            Condition.CLOCK_SKEW.render("x.csv", "HTTP 403", "ignored")
        );
        assertEquals("Unexpected internal failure reading external source", Condition.CLIENT_BUG.render("x.csv", "", ""));
        assertEquals("Unexpected internal failure reading external source: boom", Condition.CLIENT_BUG.render("", "boom", ""));
    }

    public void testObjectNameIsInsertedLiterally() {
        assertEquals("Malformed data in [a{}$1.csv]", Condition.MALFORMED_DATA.render("a{}$1.csv", "", ""));
    }

    private static void assertRendered(Condition condition, String withObject, String withoutObject) {
        assertEquals(withObject, condition.render("x.csv", "", ""));
        assertEquals(withoutObject, condition.render("", "", ""));
    }
}
