/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;

@WithoutSecurityManager
public class TerminalTests extends ESTestCase {

    public void testSystemTerminalIfRedirected() {
        // Expect system terminal if redirected for tests.
        // To force new behavior in JDK 22 this should run without security manager.
        // Otherwise, JDK 22 doesn't provide a console if redirected.
        assertEquals(Terminal.SystemTerminal.class, Terminal.DEFAULT.getClass());
    }
}
