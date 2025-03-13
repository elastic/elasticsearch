/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import java.security.BasicPermission;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Checked by scripting engines to allow loading a java class.
 * <p>
 * Examples:
 * <p>
 * Allow permission to {@code java.util.List}
 * <pre>permission org.elasticsearch.script.ClassPermission "java.util.List";</pre>
 * Allow permission to classes underneath {@code java.util} (and its subpackages such as {@code java.util.zip})
 * <pre>permission org.elasticsearch.script.ClassPermission "java.util.*";</pre>
 * Allow permission to standard predefined list of basic classes (see list below)
 * <pre>permission org.elasticsearch.script.ClassPermission "&lt;&lt;STANDARD&gt;&gt;";</pre>
 * Allow permission to all classes
 * <pre>permission org.elasticsearch.script.ClassPermission "*";</pre>
 * <p>
 * Set of classes (allowed by special value <code>&lt;&lt;STANDARD&gt;&gt;</code>):
 * <ul>
 *   <li>{@link java.lang.Boolean}</li>
 *   <li>{@link java.lang.Byte}</li>
 *   <li>{@link java.lang.Character}</li>
 *   <li>{@link java.lang.Double}</li>
 *   <li>{@link java.lang.Integer}</li>
 *   <li>{@link java.lang.Long}</li>
 *   <li>{@link java.lang.Math}</li>
 *   <li>{@link java.lang.Object}</li>
 *   <li>{@link java.lang.Short}</li>
 *   <li>{@link java.lang.String}</li>
 *   <li>{@link java.math.BigDecimal}</li>
 *   <li>{@link java.util.ArrayList}</li>
 *   <li>{@link java.util.Arrays}</li>
 *   <li>{@link java.util.Date}</li>
 *   <li>{@link java.util.HashMap}</li>
 *   <li>{@link java.util.HashSet}</li>
 *   <li>{@link java.util.Iterator}</li>
 *   <li>{@link java.util.List}</li>
 *   <li>{@link java.util.Map}</li>
 *   <li>{@link java.util.Set}</li>
 *   <li>{@link java.util.UUID}</li>
 * </ul>
 */
public final class ClassPermission extends BasicPermission {
    public static final String STANDARD = "<<STANDARD>>";
    // jdk classes
    /** Typical set of classes for scripting: basic data types, math, dates, and simple collections */
    // this is the list from the old grovy sandbox impl (+ some things like String, Iterator, etc that were missing)
    private static final Set<String> STANDARD_CLASSES = Set.of(
        Boolean.class.getName(),
        Byte.class.getName(),
        Character.class.getName(),
        Double.class.getName(),
        Integer.class.getName(),
        Long.class.getName(),
        Math.class.getName(),
        Object.class.getName(),
        Short.class.getName(),
        String.class.getName(),
        java.math.BigDecimal.class.getName(),
        java.util.ArrayList.class.getName(),
        Arrays.class.getName(),
        java.util.Date.class.getName(),
        java.util.HashMap.class.getName(),
        HashSet.class.getName(),
        java.util.Iterator.class.getName(),
        java.util.List.class.getName(),
        java.util.Map.class.getName(),
        Set.class.getName(),
        java.util.UUID.class.getName()
    );

    /**
     * Creates a new ClassPermission object.
     *
     * @param name class to grant permission to
     */
    public ClassPermission(String name) {
        super(name);
    }

    /**
     * Creates a new ClassPermission object.
     * This constructor exists for use by the {@code Policy} object to instantiate new Permission objects.
     *
     * @param name class to grant permission to
     * @param actions ignored
     */
    public ClassPermission(String name, String actions) {
        this(name);
    }

    @Override
    public boolean implies(Permission p) {
        // check for a special value of STANDARD to imply the basic set
        if (p != null && p.getClass() == getClass()) {
            ClassPermission other = (ClassPermission) p;
            if (STANDARD.equals(getName()) && STANDARD_CLASSES.contains(other.getName())) {
                return true;
            }
        }
        return super.implies(p);
    }

    @Override
    public PermissionCollection newPermissionCollection() {
        // BasicPermissionCollection only handles wildcards, we expand <<STANDARD>> here
        PermissionCollection impl = super.newPermissionCollection();
        return new PermissionCollection() {
            @Override
            public void add(Permission permission) {
                if (permission instanceof ClassPermission && STANDARD.equals(permission.getName())) {
                    for (String clazz : STANDARD_CLASSES) {
                        impl.add(new ClassPermission(clazz));
                    }
                } else {
                    impl.add(permission);
                }
            }

            @Override
            public boolean implies(Permission permission) {
                return impl.implies(permission);
            }

            @Override
            public Enumeration<Permission> elements() {
                return impl.elements();
            }
        };
    }
}
