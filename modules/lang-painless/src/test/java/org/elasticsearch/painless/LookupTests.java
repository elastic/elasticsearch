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
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;

public class LookupTests extends ESTestCase {

    protected PainlessLookup painlessLookup;

    @Before
    public void setup() {
        painlessLookup = PainlessLookupBuilder.buildFromWhitelists(Collections.singletonList(
                WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.lookup")
        ));
    }

    public static class A { }           // in whitelist
    public static class B extends A { } // not in whitelist
    public static class C extends B { } // in whitelist
    public static class D extends B { } // in whitelist

    public interface Z { }              // in whitelist
    public interface Y { }              // not in whitelist
    public interface X extends Y, Z { } // not in whitelist
    public interface V extends Y, Z { } // in whitelist
    public interface U extends X { }    // in whitelist
    public interface T extends V { }    // in whitelist
    public interface S extends U, X { } // in whitelist

    public static class AA implements X { }            // in whitelist
    public static class AB extends AA implements S { } // not in whitelist
    public static class AC extends AB implements V { } // in whitelist
    public static class AD implements X, S, T { }      // in whitelist

    public void testDirectSubClasses() {
        Set<Class<?>> directSubClasses = painlessLookup.getDirectSubClasses(Object.class);
        assertEquals(4, directSubClasses.size());
        assertTrue(directSubClasses.contains(A.class));
        assertTrue(directSubClasses.contains(Z.class));
        assertTrue(directSubClasses.contains(AA.class));
        assertTrue(directSubClasses.contains(AD.class));

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
        assertEquals(1, directSubClasses.size());
        assertTrue(directSubClasses.contains(AC.class));

        directSubClasses = painlessLookup.getDirectSubClasses(AB.class);
        assertNull(directSubClasses);

        directSubClasses = painlessLookup.getDirectSubClasses(AC.class);
        assertTrue(directSubClasses.isEmpty());

        directSubClasses = painlessLookup.getDirectSubClasses(AD.class);
        assertTrue(directSubClasses.isEmpty());
    }
}
