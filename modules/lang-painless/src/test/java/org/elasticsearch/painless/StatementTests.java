/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class StatementTests extends ScriptTestCase {
    // Ensure that new object creation without a read does not fail.
    public void testMethodDup() {
        assertEquals(1, exec("int i = 1; new ArrayList(new HashSet()); return i;"));
        assertEquals(1, exec("new HashSet(); return 1;"));
        assertEquals(1, exec("void foo() { new HashMap(); new ArrayList(); } return 1;"));
    }
}
