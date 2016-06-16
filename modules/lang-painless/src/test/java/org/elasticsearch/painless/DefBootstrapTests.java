package org.elasticsearch.painless;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.elasticsearch.test.ESTestCase;

public class DefBootstrapTests extends ESTestCase {
    
    /** calls toString() on integers, twice */
    public void testOneType() throws Throwable {
        CallSite site = DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                  "toString", 
                                                  MethodType.methodType(String.class, Object.class), 
                                                  DefBootstrap.METHOD_CALL, 0L);
        MethodHandle handle = site.dynamicInvoker();
        assertDepthEquals(site, 0);

        // invoke with integer, needs lookup
        assertEquals("5", (String)handle.invokeExact((Object)5));
        assertDepthEquals(site, 1);

        // invoked with integer again: should be cached
        assertEquals("6", (String)handle.invokeExact((Object)6));
        assertDepthEquals(site, 1);
    }
    
    public void testTwoTypes() throws Throwable {
        CallSite site = DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                  "toString", 
                                                  MethodType.methodType(String.class, Object.class), 
                                                  DefBootstrap.METHOD_CALL, 0L);
        MethodHandle handle = site.dynamicInvoker();
        assertDepthEquals(site, 0);

        assertEquals("5", (String)handle.invokeExact((Object)5));
        assertDepthEquals(site, 1);
        assertEquals("1.5", (String)handle.invokeExact((Object)1.5f));
        assertDepthEquals(site, 2);

        // both these should be cached
        assertEquals("6", (String)handle.invokeExact((Object)6));
        assertDepthEquals(site, 2);
        assertEquals("2.5", (String)handle.invokeExact((Object)2.5f));
        assertDepthEquals(site, 2);
    }
    
    public void testTooManyTypes() throws Throwable {
        // if this changes, test must be rewritten
        assertEquals(5, DefBootstrap.PIC.MAX_DEPTH);
        CallSite site = DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                  "toString", 
                                                  MethodType.methodType(String.class, Object.class), 
                                                  DefBootstrap.METHOD_CALL, 0L);
        MethodHandle handle = site.dynamicInvoker();
        assertDepthEquals(site, 0);

        assertEquals("5", (String)handle.invokeExact((Object)5));
        assertDepthEquals(site, 1);
        assertEquals("1.5", (String)handle.invokeExact((Object)1.5f));
        assertDepthEquals(site, 2);
        assertEquals("6", (String)handle.invokeExact((Object)6L));
        assertDepthEquals(site, 3);
        assertEquals("3.2", (String)handle.invokeExact((Object)3.2d));
        assertDepthEquals(site, 4);
        assertEquals("foo", (String)handle.invokeExact((Object)"foo"));
        assertDepthEquals(site, 5);
        assertEquals("c", (String)handle.invokeExact((Object)'c'));
        assertDepthEquals(site, 5);
    }
    
    /** test that we revert to the megamorphic classvalue cache and that it works as expected */
    public void testMegamorphic() throws Throwable {
        DefBootstrap.PIC site = (DefBootstrap.PIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                                          "size", 
                                                                          MethodType.methodType(int.class, Object.class), 
                                                                          DefBootstrap.METHOD_CALL, 0L);
        site.depth = DefBootstrap.PIC.MAX_DEPTH; // mark megamorphic
        MethodHandle handle = site.dynamicInvoker();
        assertEquals(2, (int)handle.invokeExact((Object) Arrays.asList("1", "2")));
        assertEquals(1, (int)handle.invokeExact((Object) Collections.singletonMap("a", "b")));
        assertEquals(3, (int)handle.invokeExact((Object) Arrays.asList("x", "y", "z")));
        assertEquals(2, (int)handle.invokeExact((Object) Arrays.asList("u", "v")));
        
        final HashMap<String,String> map = new HashMap<String,String>();
        map.put("x", "y");
        map.put("a", "b");
        assertEquals(2, (int)handle.invokeExact((Object) map));
        
        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> {
            Integer.toString((int)handle.invokeExact(new Object()));
        });
        assertEquals("Unable to find dynamic method [size] with [0] arguments for class [java.lang.Object].", iae.getMessage());
        assertTrue("Does not fail inside ClassValue.computeValue()", Arrays.stream(iae.getStackTrace()).anyMatch(e -> {
            return e.getMethodName().equals("computeValue") &&
                   e.getClassName().startsWith("org.elasticsearch.painless.DefBootstrap$PIC$");
        }));
    }
    
    // test operators with null guards

    public void testNullGuardAdd() throws Throwable {
        DefBootstrap.MIC site = (DefBootstrap.MIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                               "add", 
                                                               MethodType.methodType(Object.class, Object.class, Object.class),
                                                               DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
        MethodHandle handle = site.dynamicInvoker();
        assertEquals("nulltest", (Object)handle.invokeExact((Object)null, (Object)"test"));
    }
    
    public void testNullGuardAddWhenCached() throws Throwable {
        DefBootstrap.MIC site = (DefBootstrap.MIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                               "add", 
                                                               MethodType.methodType(Object.class, Object.class, Object.class),
                                                               DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
        MethodHandle handle = site.dynamicInvoker();
        assertEquals(2, (Object)handle.invokeExact((Object)1, (Object)1));
        assertEquals("nulltest", (Object)handle.invoke((Object)null, (Object)"test"));
    }
    
    public void testNullGuardEq() throws Throwable {
        DefBootstrap.MIC site = (DefBootstrap.MIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                               "eq", 
                                                               MethodType.methodType(boolean.class, Object.class, Object.class),
                                                               DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
        MethodHandle handle = site.dynamicInvoker();
        assertFalse((boolean) handle.invokeExact((Object)null, (Object)"test"));
        assertTrue((boolean) handle.invokeExact((Object)null, (Object)null));
    }
    
    public void testNullGuardEqWhenCached() throws Throwable {
        DefBootstrap.MIC site = (DefBootstrap.MIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                               "eq", 
                                                               MethodType.methodType(boolean.class, Object.class, Object.class),
                                                               DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
        MethodHandle handle = site.dynamicInvoker();
        assertTrue((boolean) handle.invokeExact((Object)1, (Object)1));
        assertFalse((boolean) handle.invokeExact((Object)null, (Object)"test"));
        assertTrue((boolean) handle.invokeExact((Object)null, (Object)null));
    }
    
    // make sure these operators work without null guards too
    // for example, nulls are only legal for + if the other parameter is a String,
    // and can be disabled in some circumstances.
    
    public void testNoNullGuardAdd() throws Throwable {
        DefBootstrap.MIC site = (DefBootstrap.MIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                               "add", 
                                                               MethodType.methodType(Object.class, int.class, Object.class),
                                                               DefBootstrap.BINARY_OPERATOR, 0);
        MethodHandle handle = site.dynamicInvoker();
        expectThrows(NullPointerException.class, () -> {
            assertNotNull((Object)handle.invokeExact(5, (Object)null));
        });
    }
    
    public void testNoNullGuardAddWhenCached() throws Throwable {
        DefBootstrap.MIC site = (DefBootstrap.MIC) DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                               "add", 
                                                               MethodType.methodType(Object.class, int.class, Object.class),
                                                               DefBootstrap.BINARY_OPERATOR, 0);
        MethodHandle handle = site.dynamicInvoker();
        assertEquals(2, (Object)handle.invokeExact(1, (Object)1));
        expectThrows(NullPointerException.class, () -> {
            assertNotNull((Object)handle.invokeExact(5, (Object)null));
        });
    }
    
    static void assertDepthEquals(CallSite site, int expected) {
        DefBootstrap.PIC dsite = (DefBootstrap.PIC) site;
        assertEquals(expected, dsite.depth);
    }
}
