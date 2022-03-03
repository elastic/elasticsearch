/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class LookupTests extends ESTestCase {

    protected PainlessLookup painlessLookup;

    @Before
    public void setup() {
        painlessLookup = PainlessLookupBuilder.buildFromWhitelists(
            Collections.singletonList(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.lookup"))
        );
    }

    public static class A {}                                                   // in whitelist

    public static class B extends A {}                                         // not in whitelist

    public static class C extends B {                                           // in whitelist
        public String getString0() {
            return "C/0";
        }                                // in whitelist
    }

    public static class D extends B {                                           // in whitelist
        public String getString0() {
            return "D/0";
        }                                // in whitelist

        public String getString1(int param0) {
            return "D/1 (" + param0 + ")";
        }     // in whitelist
    }

    public interface Z {}              // in whitelist

    public interface Y {}              // not in whitelist

    public interface X extends Y, Z {} // not in whitelist

    public interface V extends Y, Z {} // in whitelist

    public interface U extends X {      // in whitelist
        String getString2(int x, int y);    // in whitelist

        String getString1(int param0);      // in whitelist

        String getString0();                // not in whitelist
    }

    public interface T extends V {      // in whitelist
        String getString1(int param0);      // in whitelist

        int getInt0();                      // in whitelist
    }

    public interface S extends U, X {} // in whitelist

    public static class AA implements X {}                           // in whitelist

    public static class AB extends AA implements S {                  // not in whitelist
        public String getString2(int x, int y) {
            return "" + x + y;
        }     // not in whitelist

        public String getString1(int param0) {
            return "" + param0;
        }      // not in whitelist

        public String getString0() {
            return "";
        }                         // not in whitelist
    }

    public static class AC extends AB implements V {                  // in whitelist
        public String getString2(int x, int y) {
            return "" + x + y;
        }     // in whitelist
    }

    public static class AD extends AA implements X, S, T {            // in whitelist
        public String getString2(int x, int y) {
            return "" + x + y;
        }     // in whitelist

        public String getString1(int param0) {
            return "" + param0;
        }      // in whitelist

        public String getString0() {
            return "";
        }                         // not in whitelist

        public int getInt0() {
            return 0;
        }                                // in whitelist
    }

    public void testDirectSubClasses() {
        Set<Class<?>> directSubClasses = painlessLookup.getDirectSubClasses(Object.class);
        assertEquals(4, directSubClasses.size());
        assertTrue(directSubClasses.contains(String.class));
        assertTrue(directSubClasses.contains(A.class));
        assertTrue(directSubClasses.contains(Z.class));
        assertTrue(directSubClasses.contains(AA.class));

        directSubClasses = painlessLookup.getDirectSubClasses(A.class);
        assertEquals(2, directSubClasses.size());
        assertTrue(directSubClasses.contains(D.class));
        assertTrue(directSubClasses.contains(C.class));

        directSubClasses = painlessLookup.getDirectSubClasses(B.class);
        assertNull(directSubClasses);

        directSubClasses = painlessLookup.getDirectSubClasses(C.class);
        assertTrue(directSubClasses.isEmpty());

        directSubClasses = painlessLookup.getDirectSubClasses(D.class);
        assertTrue(directSubClasses.isEmpty());

        directSubClasses = painlessLookup.getDirectSubClasses(Z.class);
        assertEquals(5, directSubClasses.size());
        assertTrue(directSubClasses.contains(V.class));
        assertTrue(directSubClasses.contains(U.class));
        assertTrue(directSubClasses.contains(S.class));
        assertTrue(directSubClasses.contains(AA.class));
        assertTrue(directSubClasses.contains(AD.class));

        directSubClasses = painlessLookup.getDirectSubClasses(Y.class);
        assertNull(directSubClasses);

        directSubClasses = painlessLookup.getDirectSubClasses(X.class);
        assertNull(directSubClasses);

        directSubClasses = painlessLookup.getDirectSubClasses(V.class);
        assertEquals(2, directSubClasses.size());
        assertTrue(directSubClasses.contains(T.class));
        assertTrue(directSubClasses.contains(AC.class));

        directSubClasses = painlessLookup.getDirectSubClasses(U.class);
        assertEquals(1, directSubClasses.size());
        assertTrue(directSubClasses.contains(S.class));

        directSubClasses = painlessLookup.getDirectSubClasses(T.class);
        assertEquals(1, directSubClasses.size());
        assertTrue(directSubClasses.contains(AD.class));

        directSubClasses = painlessLookup.getDirectSubClasses(S.class);
        assertEquals(2, directSubClasses.size());
        assertTrue(directSubClasses.contains(AC.class));
        assertTrue(directSubClasses.contains(AD.class));

        directSubClasses = painlessLookup.getDirectSubClasses(AA.class);
        assertEquals(2, directSubClasses.size());
        assertTrue(directSubClasses.contains(AC.class));
        assertTrue(directSubClasses.contains(AD.class));

        directSubClasses = painlessLookup.getDirectSubClasses(AB.class);
        assertNull(directSubClasses);

        directSubClasses = painlessLookup.getDirectSubClasses(AC.class);
        assertTrue(directSubClasses.isEmpty());

        directSubClasses = painlessLookup.getDirectSubClasses(AD.class);
        assertTrue(directSubClasses.isEmpty());
    }

    public void testDirectSubClassMethods() {
        PainlessMethod CgetString0 = painlessLookup.lookupPainlessMethod(C.class, false, "getString0", 0);
        PainlessMethod DgetString0 = painlessLookup.lookupPainlessMethod(D.class, false, "getString0", 0);
        List<PainlessMethod> subMethods = painlessLookup.lookupPainlessSubClassesMethod(A.class, "getString0", 0);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(CgetString0));
        assertTrue(subMethods.contains(DgetString0));

        PainlessMethod DgetString1 = painlessLookup.lookupPainlessMethod(D.class, false, "getString1", 1);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(A.class, "getString1", 1);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(DgetString1));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(A.class, "getString2", 0);
        assertNull(subMethods);

        PainlessMethod ACgetString2 = painlessLookup.lookupPainlessMethod(AC.class, false, "getString2", 2);
        PainlessMethod ADgetString2 = painlessLookup.lookupPainlessMethod(AD.class, false, "getString2", 2);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(AA.class, "getString2", 2);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(ACgetString2));
        assertTrue(subMethods.contains(ADgetString2));

        PainlessMethod ADgetString1 = painlessLookup.lookupPainlessMethod(AD.class, false, "getString1", 1);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(AA.class, "getString1", 1);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(ADgetString1));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(AA.class, "getString0", 0);
        assertNull(subMethods);

        PainlessMethod ADgetInt0 = painlessLookup.lookupPainlessMethod(AD.class, false, "getInt0", 0);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(AA.class, "getInt0", 0);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(ADgetInt0));

        PainlessMethod UgetString2 = painlessLookup.lookupPainlessMethod(U.class, false, "getString2", 2);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(Z.class, "getString2", 2);
        assertNotNull(subMethods);
        assertEquals(3, subMethods.size());
        assertTrue(subMethods.contains(UgetString2));
        assertTrue(subMethods.contains(ACgetString2));
        assertTrue(subMethods.contains(ADgetString2));

        PainlessMethod UgetString1 = painlessLookup.lookupPainlessMethod(U.class, false, "getString1", 1);
        PainlessMethod TgetString1 = painlessLookup.lookupPainlessMethod(T.class, false, "getString1", 1);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(Z.class, "getString1", 1);
        assertNotNull(subMethods);
        assertEquals(3, subMethods.size());
        assertTrue(subMethods.contains(UgetString1));
        assertTrue(subMethods.contains(TgetString1));
        assertTrue(subMethods.contains(ADgetString1));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(Z.class, "getString0", 0);
        assertNull(subMethods);

        PainlessMethod TgetInt0 = painlessLookup.lookupPainlessMethod(T.class, false, "getInt0", 0);
        subMethods = painlessLookup.lookupPainlessSubClassesMethod(Z.class, "getInt0", 0);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(TgetInt0));
        assertTrue(subMethods.contains(ADgetInt0));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(V.class, "getString2", 2);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(ACgetString2));
        assertTrue(subMethods.contains(ADgetString2));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(V.class, "getString1", 1);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(TgetString1));
        assertTrue(subMethods.contains(ADgetString1));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(V.class, "getString0", 0);
        assertNull(subMethods);

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(V.class, "getInt0", 0);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(TgetInt0));
        assertTrue(subMethods.contains(ADgetInt0));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(U.class, "getString2", 2);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(ACgetString2));
        assertTrue(subMethods.contains(ADgetString2));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(U.class, "getString1", 1);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(ADgetString1));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(U.class, "getString0", 0);
        assertNull(subMethods);

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(U.class, "getInt0", 0);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(ADgetInt0));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(V.class, "getInt0", 0);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(TgetInt0));
        assertTrue(subMethods.contains(ADgetInt0));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(S.class, "getString2", 2);
        assertNotNull(subMethods);
        assertEquals(2, subMethods.size());
        assertTrue(subMethods.contains(ACgetString2));
        assertTrue(subMethods.contains(ADgetString2));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(S.class, "getString1", 1);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(ADgetString1));

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(S.class, "getString0", 0);
        assertNull(subMethods);

        subMethods = painlessLookup.lookupPainlessSubClassesMethod(S.class, "getInt0", 0);
        assertNotNull(subMethods);
        assertEquals(1, subMethods.size());
        assertTrue(subMethods.contains(ADgetInt0));
    }
}
