/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.test.ESTestCase;

public class TerminalTests extends ESTestCase {

    public void testSystemTerminalIfRedirected() {
        // expect system terminal if redirected for tests
        assertEquals(Terminal.SystemTerminal.class, Terminal.DEFAULT.getClass());
    }
}
