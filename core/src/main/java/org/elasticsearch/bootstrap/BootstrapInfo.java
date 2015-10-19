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

/** 
 * Exposes system startup information 
 */
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
     * Returns true if secure computing mode is enabled (supported systems only)
     */
    public static boolean isSeccompInstalled() {
        return Natives.isSeccompInstalled();
    }

    /**
     * codebase location for untrusted scripts (provide some additional safety)
     * <p>
     * This is not a full URL, just a path.
     */
    public static final String UNTRUSTED_CODEBASE = "/untrusted";
}
