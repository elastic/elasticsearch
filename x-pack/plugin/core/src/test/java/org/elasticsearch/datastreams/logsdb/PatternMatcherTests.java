/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.logsdb;

import junit.framework.TestCase;

public class PatternMatcherTests extends TestCase {

    private static final PatternMatcher matcher = PatternMatcher.forLogs();

    public void testMatching() {
        assertTrue(matcher.matches("logs-apache-production"));
        assertTrue(matcher.matches("logs-apache-123"));
        assertTrue(matcher.matches("logs-123-production"));
        assertTrue(matcher.matches("logs-apache.kafka-production.eu.west"));
    }

    public void testNonMatching() {
        assertFalse(matcher.matches(null));
        assertFalse(matcher.matches(""));
        assertFalse(matcher.matches("logs"));
        assertFalse(matcher.matches("logs-apache"));
        assertFalse(matcher.matches("standard-apache-production"));
        assertFalse(matcher.matches("logs-apache-production-eu"));
    }

}
