/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase.versionMayDropWarnings;

public class EsqlSpecTestCaseTests extends ESTestCase {

    public void testVersionsBeforeDriverWarningSinkMayDropWarnings() {
        for (String version : new String[] {
            "8.18.8",
            "8.19.22",
            "9.0.0",
            "9.0.8",
            "9.1.9",
            "9.2.8",
            "9.3.8",
            "9.4.0",
            "9.4.7",
            "9.5.0",
            "9.5.4" }) {
            assertTrue(version, versionMayDropWarnings(Version.fromString(version)));
        }
    }

    public void testVersionsWithDriverWarningSinkKeepWarnings() {
        for (String version : new String[] { "8.19.23", "8.19.24", "9.4.8", "9.4.9", "9.5.5", "9.5.6", "9.6.0" }) {
            assertFalse(version, versionMayDropWarnings(Version.fromString(version)));
        }
        assertFalse(versionMayDropWarnings(Version.CURRENT));
    }
}
