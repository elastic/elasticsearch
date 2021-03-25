/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.toxcontent.UserTreeToXContent;

public class UserFunctionTests extends ScriptTestCase {
    public void testZeroArgumentUserFunction() {
        String source = "def twofive() { return 25; } twofive()";
        assertEquals(25, exec(source));
        UserTreeVisitor<ScriptScope> semantic = new UserTreeToXContent();
        UserTreeVisitor<ScriptScope> ir = new UserTreeToXContent();
        Debugger.phases(source, semantic, ir, null);
        System.out.println("----------Semantic----------");
        System.out.println(semantic);
        System.out.println("----------IR----------");
        System.out.println(ir);
    }
}
