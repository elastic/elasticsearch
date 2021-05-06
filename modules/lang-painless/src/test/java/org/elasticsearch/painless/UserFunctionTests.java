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

import java.util.List;

public class UserFunctionTests extends ScriptTestCase {
    public void testZeroArgumentUserFunction() {
        String source = "def twofive() { return 25; } twofive()";
        assertEquals(25, exec(source));
    }

    public void testUserFunctionDefCallRef() {
        String source =
                "int myCompare(int a, int b) { getMulti() * Integer.compare(a, b) }\n" +
                "int getMulti() { return -1 }\n" +
                "List l = [1, 100, -100];\n" +
                "if (myCompare(10, 50) > 0) { l.add(50 + getMulti()) }\n" +
                "l.sort(this::myCompare);\n" +
                "if (l[0] == 100) { l.remove(l.size() - 1) ; l.sort((a, b) -> -1 * myCompare(a, b)) } \n"+
                //"BiFunction f = (a, b) -> -1 * myCompare(a, b);\n "+
                //"if (f(50, 100) > 0) { l.add(51) }\n" +
                "return l;";
        System.out.println(source);
        System.out.print(Debugger.toString(source));
        //phases(source);
        assertEquals(List.of(1, 49, 100), exec(source));
    }

    public void phases(String script) {
        UserTreeVisitor<ScriptScope> semantic = new UserTreeToXContent();
        UserTreeVisitor<ScriptScope> ir = new UserTreeToXContent();
        try {
            Debugger.phases(script, semantic, ir, null);
            System.out.println("----------Semantic----------");
            System.out.println(semantic);
            System.out.println("----------IR----------");
            System.out.println(ir);
        } catch (RuntimeException err) {
            System.out.println("err [" + err.getMessage() + "]");
        }
    }
}
