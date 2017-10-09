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

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import java.security.AllPermission;
import java.security.PermissionCollection;

/** Very simple sanity checks for {@link ClassPermission} */
public class ClassPermissionTests extends ESTestCase {

    public void testEquals() {
        assertEquals(new ClassPermission("pkg.MyClass"), new ClassPermission("pkg.MyClass"));
        assertFalse(new ClassPermission("pkg.MyClass").equals(new AllPermission()));
    }

    public void testImplies() {
        assertTrue(new ClassPermission("pkg.MyClass").implies(new ClassPermission("pkg.MyClass")));
        assertFalse(new ClassPermission("pkg.MyClass").implies(new ClassPermission("pkg.MyOtherClass")));
        assertFalse(new ClassPermission("pkg.MyClass").implies(null));
        assertFalse(new ClassPermission("pkg.MyClass").implies(new AllPermission()));
    }

    public void testStandard() {
        assertTrue(new ClassPermission("<<STANDARD>>").implies(new ClassPermission("java.lang.Math")));
        assertFalse(new ClassPermission("<<STANDARD>>").implies(new ClassPermission("pkg.MyClass")));
    }
 
    public void testPermissionCollection() {
        ClassPermission math = new ClassPermission("java.lang.Math");
        PermissionCollection collection = math.newPermissionCollection();
        collection.add(math);
        assertTrue(collection.implies(new ClassPermission("java.lang.Math")));
        assertFalse(collection.implies(new ClassPermission("pkg.MyClass")));
    }
    
    public void testPermissionCollectionStandard() {
        ClassPermission standard = new ClassPermission("<<STANDARD>>");
        PermissionCollection collection = standard.newPermissionCollection();
        collection.add(standard);
        assertTrue(collection.implies(new ClassPermission("java.lang.Math")));
        assertFalse(collection.implies(new ClassPermission("pkg.MyClass")));
    }

    /** not recommended but we test anyway */
    public void testWildcards() {
        assertTrue(new ClassPermission("*").implies(new ClassPermission("pkg.MyClass")));
        assertTrue(new ClassPermission("pkg.*").implies(new ClassPermission("pkg.MyClass")));
        assertTrue(new ClassPermission("pkg.*").implies(new ClassPermission("pkg.sub.MyClass")));
        assertFalse(new ClassPermission("pkg.My*").implies(new ClassPermission("pkg.MyClass")));
        assertFalse(new ClassPermission("pkg*").implies(new ClassPermission("pkg.MyClass")));        
    }
    
    public void testPermissionCollectionWildcards() {
        ClassPermission lang = new ClassPermission("java.lang.*");
        PermissionCollection collection = lang.newPermissionCollection();
        collection.add(lang);
        assertTrue(collection.implies(new ClassPermission("java.lang.Math")));
        assertFalse(collection.implies(new ClassPermission("pkg.MyClass")));
    }
}
