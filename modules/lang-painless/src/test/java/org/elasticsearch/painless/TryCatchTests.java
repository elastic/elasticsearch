package org.elasticsearch.painless;

import java.util.Collections;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/** tests for throw/try/catch in painless */
public class TryCatchTests extends ScriptTestCase {

    /** throws an exception */
    public void testThrow() {
        RuntimeException exception = expectScriptThrows(RuntimeException.class, () -> {
            exec("throw new RuntimeException('test')");
        });
        assertEquals("test", exception.getMessage());
    }

    /** catches the exact exception */
    public void testCatch() {
        assertEquals(1, exec("try { if (params.param == 'true') throw new RuntimeException('test'); } " +
                             "catch (RuntimeException e) { return 1; } return 2;",
                              Collections.singletonMap("param", "true"), true));
    }

    /** catches superclass of the exception */
    public void testCatchSuperclass() {
        assertEquals(1, exec("try { if (params.param == 'true') throw new RuntimeException('test'); } " +
                             "catch (Exception e) { return 1; } return 2;",
                              Collections.singletonMap("param", "true"), true));
    }

    /** tries to catch a different type of exception */
    public void testNoCatch() {
        RuntimeException exception = expectScriptThrows(RuntimeException.class, () -> {
           exec("try { if (params.param == 'true') throw new RuntimeException('test'); } " +
                "catch (ArithmeticException e) { return 1; } return 2;",
                Collections.singletonMap("param", "true"), true);
        });
        assertEquals("test", exception.getMessage());
    }

    public void testNoCatchBlock() {
        assertEquals(0, exec("try { return Integer.parseInt('f') } catch (NumberFormatException nfe) {} return 0;"));

        assertEquals(0, exec("try { return Integer.parseInt('f') } " +
                "catch (NumberFormatException nfe) {}" +
                "catch (Exception e) {}" +
                " return 0;"));

        assertEquals(0, exec("try { throw new IllegalArgumentException('test') } " +
                "catch (NumberFormatException nfe) {}" +
                "catch (Exception e) {}" +
                " return 0;"));

        assertEquals(0, exec("try { throw new IllegalArgumentException('test') } " +
                "catch (NumberFormatException nfe) {}" +
                "catch (IllegalArgumentException iae) {}" +
                "catch (Exception e) {}" +
                " return 0;"));
    }

    public void testMultiCatch() {
        assertEquals(1, exec(
                "try { return Integer.parseInt('f') } " +
                "catch (NumberFormatException nfe) {return 1;} " +
                "catch (ArrayIndexOutOfBoundsException aioobe) {return 2;} " +
                "catch (Exception e) {return 3;}"
        ));

        assertEquals(2, exec(
                "try { return new int[] {}[0] } " +
                "catch (NumberFormatException nfe) {return 1;} " +
                "catch (ArrayIndexOutOfBoundsException aioobe) {return 2;} " +
                "catch (Exception e) {return 3;}"
        ));

        assertEquals(3, exec(
                "try { throw new IllegalArgumentException('test'); } " +
                "catch (NumberFormatException nfe) {return 1;} " +
                "catch (ArrayIndexOutOfBoundsException aioobe) {return 2;} " +
                "catch (Exception e) {return 3;}"
        ));
    }
}
