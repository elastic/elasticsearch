/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;

import java.util.Dictionary;
import java.util.Enumeration;

/**
 * Exposes system startup information
 */
@SuppressForbidden(reason = "exposes read-only view of system properties")
public final class BootstrapInfo {

    /** no instantiation */
    private BootstrapInfo() {}

    /**
     * Returns true if we successfully loaded native libraries.
     * <p>
     * If this returns false, then native operations such as locking
     * memory did not work.
     */
    public static boolean isNativesAvailable() {
        return Natives.JNA_AVAILABLE;
    }

    /**
     * Returns true if we were able to lock the process's address space.
     */
    public static boolean isMemoryLocked() {
        return Natives.isMemoryLocked();
    }

    /**
     * Returns true if system call filter is installed (supported systems only)
     */
    public static boolean isSystemCallFilterInstalled() {
        return Natives.isSystemCallFilterInstalled();
    }

    /**
     * codebase location for untrusted scripts (provide some additional safety)
     * <p>
     * This is not a full URL, just a path.
     */
    public static final String UNTRUSTED_CODEBASE = "/untrusted";

    // create a view of sysprops map that does not allow modifications
    // this must be done this way (e.g. versus an actual typed map), because
    // some test methods still change properties, so whitelisted changes must
    // be reflected in this view.
    private static final Dictionary<Object,Object> SYSTEM_PROPERTIES;
    static {
        final Dictionary<Object,Object> sysprops = System.getProperties();
        SYSTEM_PROPERTIES = new Dictionary<Object,Object>() {

            @Override
            public int size() {
                return sysprops.size();
            }

            @Override
            public boolean isEmpty() {
                return sysprops.isEmpty();
            }

            @Override
            public Enumeration<Object> keys() {
                return sysprops.keys();
            }

            @Override
            public Enumeration<Object> elements() {
                return sysprops.elements();
            }

            @Override
            public Object get(Object key) {
                return sysprops.get(key);
            }

            @Override
            public Object put(Object key, Object value) {
                throw new UnsupportedOperationException("treat system properties as immutable");
            }

            @Override
            public Object remove(Object key) {
                throw new UnsupportedOperationException("treat system properties as immutable");
            }
        };
    }

    /**
     * Returns a read-only view of all system properties
     */
    public static Dictionary<Object,Object> getSystemProperties() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPropertyAccess("*");
        }
        return SYSTEM_PROPERTIES;
    }

    public static void init() {
    }

}
