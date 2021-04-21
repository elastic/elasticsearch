/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class ConstantFoldingTests extends ScriptTestCase {

    public void testUnary() {
        assertBytecodeExists("~1", "BIPUSH -2");
        assertBytecodeExists("~1000L", "LDC -1001");
        assertBytecodeExists("!true", "ICONST_0");
    }

    public void testBinary() {
        assertBytecodeExists("2*2", "ICONST_4");
        assertBytecodeExists("2L*2L", "LDC 4");
        assertBytecodeExists("2.0F*2.0F", "LDC 4.0");
        assertBytecodeExists("2.0*2.0", "LDC 4.0");
        assertBytecodeExists("2/2", "ICONST_1");
        assertBytecodeExists("2L/2L", "LCONST_1");
        assertBytecodeExists("2.0F/2.0F", "FCONST_1");
        assertBytecodeExists("2.0/2.0", "DCONST_1");
        assertBytecodeExists("2%2", "ICONST_0");
        assertBytecodeExists("2L%2L", "LCONST_0");
        assertBytecodeExists("2.0F%2.0F", "FCONST_0");
        assertBytecodeExists("2.0%2.0", "DCONST_0");
        assertBytecodeExists("2+3", "ICONST_5");
        assertBytecodeExists("2L+3L", "LDC 5");
        assertBytecodeExists("2.0F+3.0F", "LDC 5.0");
        assertBytecodeExists("2.0+3.0", "LDC 5.0");
        assertBytecodeExists("2-3", "ICONST_M1");
        assertBytecodeExists("2L-3L", "LDC -1");
        assertBytecodeExists("2.0F-3.0F", "LDC -1.0");
        assertBytecodeExists("2.0-3.0", "LDC -1.0");
        assertBytecodeExists("2<<1", "ICONST_4");
        assertBytecodeExists("2L<<1L", "LDC 4");
        assertBytecodeExists("4>>1", "ICONST_2");
        assertBytecodeExists("4L>>1L", "LDC 2");
        assertBytecodeExists("4>>>1", "ICONST_2");
        assertBytecodeExists("4L>>>1L", "LDC 2");
        assertBytecodeExists("5&3", "ICONST_1");
        assertBytecodeExists("5L&3L", "LDC 1");
        assertBytecodeExists("true^false", "ICONST_1");
        assertBytecodeExists("5^3", "BIPUSH 6");
        assertBytecodeExists("5L^3L", "LDC 6");
        assertBytecodeExists("5|3", "BIPUSH 7");
        assertBytecodeExists("5L|3L", "LDC 7");
    }

    public void testStringConcatenation() {
        assertBytecodeExists("'x' + 'y' + 'z'", "LDC \"xyz\"");
    }

    public void testBoolean() {
        assertBytecodeExists("true && false", "ICONST_0");
        assertBytecodeExists("true || false", "ICONST_1");
    }

    public void testComparison() {
        assertBytecodeExists("2==2", "ICONST_1");
        assertBytecodeExists("2L==2L", "ICONST_1");
        assertBytecodeExists("2.0F==2.0F", "ICONST_1");
        assertBytecodeExists("2.0==2.0", "ICONST_1");
        assertBytecodeExists("'x'=='x'", "ICONST_1");
        assertBytecodeExists("2!=2", "ICONST_0");
        assertBytecodeExists("2L!=2L", "ICONST_0");
        assertBytecodeExists("2.0F!=2.0F", "ICONST_0");
        assertBytecodeExists("2.0!=2.0", "ICONST_0");
        assertBytecodeExists("'x'!='x'", "ICONST_0");
        assertBytecodeExists("2===2", "ICONST_1");
        assertBytecodeExists("2L===2L", "ICONST_1");
        assertBytecodeExists("2.0F===2.0F", "ICONST_1");
        assertBytecodeExists("2.0===2.0", "ICONST_1");
        assertBytecodeExists("'x'==='x'", "ICONST_0");
        assertBytecodeExists("2!==2", "ICONST_0");
        assertBytecodeExists("2L!==2L", "ICONST_0");
        assertBytecodeExists("2.0F!==2.0F", "ICONST_0");
        assertBytecodeExists("2.0!==2.0", "ICONST_0");
        assertBytecodeExists("'x'!=='x'", "ICONST_1");
        assertBytecodeExists("2>2", "ICONST_0");
        assertBytecodeExists("2L>2L", "ICONST_0");
        assertBytecodeExists("2.0F>2.0F", "ICONST_0");
        assertBytecodeExists("2.0>2.0", "ICONST_0");
        assertBytecodeExists("2>=2", "ICONST_1");
        assertBytecodeExists("2L>=2L", "ICONST_1");
        assertBytecodeExists("2.0F>=2.0F", "ICONST_1");
        assertBytecodeExists("2.0>=2.0", "ICONST_1");
        assertBytecodeExists("2<2", "ICONST_0");
        assertBytecodeExists("2L<2L", "ICONST_0");
        assertBytecodeExists("2.0F<2.0F", "ICONST_0");
        assertBytecodeExists("2.0<2.0", "ICONST_0");
        assertBytecodeExists("2<=2", "ICONST_1");
        assertBytecodeExists("2L<=2L", "ICONST_1");
        assertBytecodeExists("2.0F<=2.0F", "ICONST_1");
        assertBytecodeExists("2.0<=2.0", "ICONST_1");
    }

    public void testCast() {
        assertBytecodeExists("2==2L", "ICONST_1");
        assertBytecodeExists("2+2D", "LDC 4.0");
        assertBytecodeExists("2+'2D'", "LDC \"22D\"");
        assertBytecodeExists("4L<5F", "ICONST_1");
    }

    public void testStoreInMap()  {
        assertBytecodeExists("Map m = [:]; m.a = 1 + 1; m.a", "ICONST_2");
    }

    public void testStoreInMapDef()  {
        assertBytecodeExists("def m = [:]; m.a = 1 + 1; m.a", "ICONST_2");
    }

    public void testStoreInList()  {
        assertBytecodeExists("List l = [null]; l.0 = 1 + 1; l.0", "ICONST_2");
    }

    public void testStoreInListDef()  {
        assertBytecodeExists("def l = [null]; l.0 = 1 + 1; l.0", "ICONST_2");
    }

    public void testStoreInArray()  {
        assertBytecodeExists("int[] a = new int[1]; a[0] = 1 + 1; a[0]", "ICONST_2");
    }

    public void testStoreInArrayDef()  {
        assertBytecodeExists("def a = new int[1]; a[0] = 1 + 1; a[0]", "ICONST_2");
    }
}
