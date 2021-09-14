/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
