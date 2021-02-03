/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class PrefixLoggerTests extends ESTestCase {
    public void testNullPrefix() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new PrefixLogger(logger, null));
        assertThat(e.getMessage(), containsString("use a regular logger"));
    }

    public void testEmptyPrefix() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new PrefixLogger(logger, ""));
        assertThat(e.getMessage(), containsString("use a regular logger"));
    }
}
