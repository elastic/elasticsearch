/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.util.List;
import java.util.Map;

public class UserFunctionTests extends ScriptTestCase {
    public void testZeroArgumentUserFunction() {
        String source = "def twofive() { return 25; } twofive()";
        assertEquals(25, exec(source));
    }

    public void testUserFunctionDefCallRef() {
        String source = """
            String getSource() { 'source'; }
            int myCompare(int a, int b) { getMulti() * Integer.compare(a, b) }
            int getMulti() { return -1 }
            def l = [1, 100, -100];
            if (myCompare(10, 50) > 0) { l.add(50 + getMulti()) }
            l.sort(this::myCompare);
            if (l[0] == 100) { l.remove(l.size() - 1) ; l.sort((a, b) -> -1 * myCompare(a, b)) }\s
            if (getSource().startsWith('sour')) { l.add(255); }
            return l;""";
        assertEquals(List.of(1, 49, 100, 255), exec(source));
        assertBytecodeExists(source, "public &getSource()Ljava/lang/String");
        assertBytecodeExists(source, "public &getMulti()I");
        assertBytecodeExists(source, "INVOKEVIRTUAL org/elasticsearch/painless/PainlessScript$Script.&getMulti ()I");
        assertBytecodeExists(source, "public &myCompare(II)I");
        assertBytecodeExists(source, "INVOKEVIRTUAL org/elasticsearch/painless/PainlessScript$Script.&myCompare (II)I");
    }

    public void testChainedUserMethods() {
        String source = """
            int myCompare(int a, int b) { getMulti() * (a - b) }
            int getMulti() { -1 }
            List l = [1, 100, -100];
            l.sort(this::myCompare);
            l;
            """;
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
    }

    public void testChainedUserMethodsLambda() {
        String source = """
            int myCompare(int a, int b) { getMulti() * (a - b) }
            int getMulti() { -1 }
            List l = [1, 100, -100];
            l.sort((a, b) -> myCompare(a, b));
            l;
            """;
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
    }

    public void testChainedUserMethodsDef() {
        String source = """
            int myCompare(int a, int b) { getMulti() * (a - b) }
            int getMulti() { -1 }
            def l = [1, 100, -100];
            l.sort(this::myCompare);
            l;
            """;
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
    }

    public void testChainedUserMethodsLambdaDef() {
        String source = """
            int myCompare(int a, int b) { getMulti() * (a - b) }
            int getMulti() { -1 }
            def l = [1, 100, -100];
            l.sort((a, b) -> myCompare(a, b));
            l;
            """;
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
    }

    public void testChainedUserMethodsLambdaCaptureDef() {
        String source = """
            int myCompare(int a, int b, int x, int m) { getMulti(m) * (a - b + x) }
            int getMulti(int m) { -1 * m }
            def l = [1, 100, -100];
            int cx = 100;
            int cm = 1;
            l.sort((a, b) -> myCompare(a, b, cx, cm));
            l;
            """;
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
    }

    public void testMethodReferenceInUserFunction() {
        String source = """
            int myCompare(int a, int b, String s) {    Map m = ['f': 5];   a - b + m.computeIfAbsent(s, this::getLength) }
            int getLength(String s) { s.length() }
            def l = [1, 0, -2];
            String s = 'g';
            l.sort((a, b) -> myCompare(a, b, s));
            l;
            """;
        assertEquals(List.of(-2, 1, 0), exec(source, Map.of("a", 1), false));
    }

    public void testUserFunctionVirtual() {
        String source = "int myCompare(int x, int y) { return -1 * (x - y)  }\n" + "return myCompare(100, 90);";
        assertEquals(-10, exec(source, Map.of("a", 1), false));
        assertBytecodeExists(source, "INVOKEVIRTUAL org/elasticsearch/painless/PainlessScript$Script.&myCompare (II)I");
    }

    public void testUserFunctionRef() {
        String source = """
            int myCompare(int x, int y) { return -1 * x - y  }
            List l = [1, 100, -100];
            l.sort(this::myCompare);
            return l;""";
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
        assertBytecodeExists(source, "public &myCompare(II)I");
    }

    public void testUserFunctionRefEmpty() {
        String source = """
            int myCompare(int x, int y) { return -1 * x - y  }
            [].sort((a, b) -> myCompare(a, b));
            """;
        assertNull(exec(source, Map.of("a", 1), false));
        assertBytecodeExists(source, "public &myCompare(II)I");
        assertBytecodeExists(source, "INVOKEVIRTUAL org/elasticsearch/painless/PainlessScript$Script.&myCompare (II)I");
    }

    public void testUserFunctionCallInLambda() {
        String source = """
            int myCompare(int x, int y) { -1 * ( x - y ) }
            List l = [1, 100, -100];
            l.sort((a, b) -> myCompare(a, b));
            return l;""";
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
        assertBytecodeExists(source, "public &myCompare(II)I");
        assertBytecodeExists(source, "INVOKEVIRTUAL org/elasticsearch/painless/PainlessScript$Script.&myCompare (II)I");
    }

    public void testUserFunctionLambdaCapture() {
        String source = """
            int myCompare(Object o, int x, int y) { return o != null ? -1 * ( x - y ) : ( x - y ) }
            List l = [1, 100, -100];
            Object q = '';
            l.sort((a, b) -> myCompare(q, a, b));
            return l;""";
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
        assertBytecodeExists(source, "public &myCompare(Ljava/lang/Object;II)I");
        assertBytecodeExists(source, "INVOKEVIRTUAL org/elasticsearch/painless/PainlessScript$Script.&myCompare (Ljava/lang/Object;II)I");
    }

    public void testLambdaCapture() {
        String source = """
            List l = [1, 100, -100];
            int q = -1;
            l.sort((a, b) -> q * ( a - b ));
            return l;""";
        assertEquals(List.of(100, 1, -100), exec(source, Map.of("a", 1), false));
        assertBytecodeExists(source, "public static synthetic lambda$synthetic$0(ILjava/lang/Object;Ljava/lang/Object;)I");
    }

    public void testCallUserMethodFromStatementWithinLambda() {
        String source = ""
            + "int test1() { return 1; }"
            + "void test(Map params) { "
            + "  int i = 0;"
            + "  params.forEach("
            + "      (k, v) -> { if (i == 0) { test1() } else { 20 } }"
            + "    );"
            + "}"
            + "test(params)";
        assertNull(exec(source, Map.of("a", 5), false));
        assertBytecodeExists(source, "public synthetic lambda$synthetic$0(ILjava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    }

    public void testCallUserMethodFromStatementWithinNestedLambda() {
        String source = ""
            + "int test1() { return 1; }"
            + "void test(Map params) { "
            + "  int i = 0;"
            + "  int j = 5;"
            + "  params.replaceAll( "
            + "    (n, m) -> {"
            + "      m.forEach("
            + "        (k, v) -> { if (i == 0) { test1() } else { 20 } }"
            + "      );"
            + "      return ['aaa': j];"
            + "    }"
            + "  );"
            + "}"
            + "Map myParams = new HashMap(params);"
            + "test(myParams);"
            + "myParams['a']['aaa']";
        assertEquals(5, exec(source, Map.of("a", Map.of("b", 1)), false));
        assertBytecodeExists(source, "public synthetic lambda$synthetic$1(IILjava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    }
}
