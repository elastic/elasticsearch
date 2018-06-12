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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.SuppressForbidden;

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
