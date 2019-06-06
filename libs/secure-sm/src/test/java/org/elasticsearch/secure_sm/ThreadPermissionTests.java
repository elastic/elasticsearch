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

package org.elasticsearch.secure_sm;

import junit.framework.TestCase;

import java.security.AllPermission;

/**
 * Simple tests for ThreadPermission
 */
public class ThreadPermissionTests extends TestCase {

    public void testEquals() {
        assertEquals(new ThreadPermission("modifyArbitraryThread"), new ThreadPermission("modifyArbitraryThread"));
        assertFalse(new ThreadPermission("modifyArbitraryThread").equals(new AllPermission()));
        assertFalse(new ThreadPermission("modifyArbitraryThread").equals(new ThreadPermission("modifyArbitraryThreadGroup"))); }

    public void testImplies() {
        assertTrue(new ThreadPermission("modifyArbitraryThread").implies(new ThreadPermission("modifyArbitraryThread")));
        assertTrue(new ThreadPermission("modifyArbitraryThreadGroup").implies(new ThreadPermission("modifyArbitraryThreadGroup")));
        assertFalse(new ThreadPermission("modifyArbitraryThread").implies(new ThreadPermission("modifyArbitraryThreadGroup")));
        assertFalse(new ThreadPermission("modifyArbitraryThreadGroup").implies(new ThreadPermission("modifyArbitraryThread")));
        assertFalse(new ThreadPermission("modifyArbitraryThread").implies(new AllPermission()));
        assertFalse(new ThreadPermission("modifyArbitraryThreadGroup").implies(new AllPermission()));
        assertTrue(new ThreadPermission("*").implies(new ThreadPermission("modifyArbitraryThread")));
        assertTrue(new ThreadPermission("*").implies(new ThreadPermission("modifyArbitraryThreadGroup")));
        assertFalse(new ThreadPermission("*").implies(new AllPermission()));
    }
}
