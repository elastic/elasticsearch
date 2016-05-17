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

import org.elasticsearch.test.ESTestCase;

public class DefBootstrapTests extends ESTestCase {
    
    /** calls toString() on integers, twice */
    public void testOneType() throws Throwable {
        CallSite site = DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                  "toString", 
                                                  MethodType.methodType(String.class, Object.class), 
                                                  DefBootstrap.METHOD_CALL);
        MethodHandle handle = site.dynamicInvoker();
        assertDepthEquals(site, 0);

        // invoke with integer, needs lookup
        assertEquals("5", handle.invoke(Integer.valueOf(5)));
        assertDepthEquals(site, 1);

        // invoked with integer again: should be cached
        assertEquals("6", handle.invoke(Integer.valueOf(6)));
        assertDepthEquals(site, 1);
    }
    
    public void testTwoTypes() throws Throwable {
        CallSite site = DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                  "toString", 
                                                  MethodType.methodType(String.class, Object.class), 
                                                  DefBootstrap.METHOD_CALL);
        MethodHandle handle = site.dynamicInvoker();
        assertDepthEquals(site, 0);

        assertEquals("5", handle.invoke(Integer.valueOf(5)));
        assertDepthEquals(site, 1);
        assertEquals("1.5", handle.invoke(Float.valueOf(1.5f)));
        assertDepthEquals(site, 2);

        // both these should be cached
        assertEquals("6", handle.invoke(Integer.valueOf(6)));
        assertDepthEquals(site, 2);
        assertEquals("2.5", handle.invoke(Float.valueOf(2.5f)));
        assertDepthEquals(site, 2);
    }
    
    public void testTooManyTypes() throws Throwable {
        // if this changes, test must be rewritten
        assertEquals(5, DefBootstrap.PIC.MAX_DEPTH);
        CallSite site = DefBootstrap.bootstrap(MethodHandles.publicLookup(), 
                                                  "toString", 
                                                  MethodType.methodType(String.class, Object.class), 
                                                  DefBootstrap.METHOD_CALL);
        MethodHandle handle = site.dynamicInvoker();
        assertDepthEquals(site, 0);

        assertEquals("5", handle.invoke(Integer.valueOf(5)));
        assertDepthEquals(site, 1);
        assertEquals("1.5", handle.invoke(Float.valueOf(1.5f)));
        assertDepthEquals(site, 2);
        assertEquals("6", handle.invoke(Long.valueOf(6)));
        assertDepthEquals(site, 3);
        assertEquals("3.2", handle.invoke(Double.valueOf(3.2d)));
        assertDepthEquals(site, 4);
        assertEquals("foo", handle.invoke(new String("foo")));
        assertDepthEquals(site, 5);
        assertEquals("c", handle.invoke(Character.valueOf('c')));
        assertDepthEquals(site, 5);
    }
    
    static void assertDepthEquals(CallSite site, int expected) {
        DefBootstrap.PIC dsite = (DefBootstrap.PIC) site;
        assertEquals(expected, dsite.depth);
    }
}
