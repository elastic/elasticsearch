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
        assertRendered(Condition.OBJECT_ARCHIVED, "External data object [x.csv] is archived", "External data object is archived");
        assertRendered(Condition.MALFORMED_DATA, "Malformed data in [x.csv]", "Malformed external data");
        assertRendered(Condition.METADATA_UNAVAILABLE, "Failed to get metadata for [x.csv]", "Failed to get external data metadata");
        assertRendered(Condition.LISTING_FAILED, "Failed to list external data objects", "Failed to list external data objects");
        assertRendered(
            Condition.LOCAL_CAPACITY,
            "External read concurrency limit reached on this node reading [x.csv]",
            "External read concurrency limit reached on this node"
        );
    }

    public void testDetailCodeAndRemedyAreAppended() {
        assertEquals(
            "Access denied reading [x.csv] (HTTP 403 AccessDenied). Check the credentials.",
            Condition.ACCESS_DENIED.render("x.csv", "HTTP 403 AccessDenied", "Check the credentials.")
        );
        assertEquals("External store throttled (HTTP 503)", Condition.STORE_THROTTLED.render("", "HTTP 503", ""));
        assertEquals("Failed to list external data objects. Retry.", Condition.LISTING_FAILED.render("x.csv", "", "Retry."));
    }

    /**
     * Every object condition appends its detail code and remedy, so a caller's are never silently dropped.
     */
    public void testDetailCodeAndRemedyAreAppendedForEveryObjectCondition() {
        assertEquals(
            "External data object not found: [x.csv] (HTTP 404). Check the path.",
            Condition.OBJECT_NOT_FOUND.render("x.csv", "HTTP 404", "Check the path.")
        );
        assertEquals(
            "External data object [x.csv] was modified during read (HTTP 412). Re-run the query.",
            Condition.OBJECT_CHANGED.render("x.csv", "HTTP 412", "Re-run the query.")
        );
        assertEquals(
            "External data object [x.csv] is archived (HTTP 403 InvalidObjectState). Restore it.",
            Condition.OBJECT_ARCHIVED.render("x.csv", "HTTP 403 InvalidObjectState", "Restore it.")
        );
        assertEquals(
            "Malformed data in [x.csv] (bad magic). Check the format.",
            Condition.MALFORMED_DATA.render("x.csv", "bad magic", "Check the format.")
        );
        assertEquals(
            "Failed to get metadata for [x.csv] (HTTP 500). Retry.",
            Condition.METADATA_UNAVAILABLE.render("x.csv", "HTTP 500", "Retry.")
        );
        assertEquals("Failed to get external data metadata (HTTP 500)", Condition.METADATA_UNAVAILABLE.render("", "HTTP 500", ""));
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
