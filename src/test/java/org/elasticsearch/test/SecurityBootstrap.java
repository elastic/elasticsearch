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

package org.elasticsearch.test;

import org.apache.lucene.util.TestSecurityManager;
import org.elasticsearch.bootstrap.Bootstrap;
import org.elasticsearch.bootstrap.ESPolicy;
import org.elasticsearch.bootstrap.Security;
import org.elasticsearch.common.io.PathUtils;

import java.security.Permissions;
import java.security.Policy;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;

/** 
 * Installs test security manager (ensures it happens regardless of which
 * test case happens to be first, test ordering, etc). 
 * <p>
 * The idea is to mimic as much as possible what happens with ES in production
 * mode (e.g. assign permissions and install security manager the same way)
 */
class SecurityBootstrap {
    
    // TODO: can we share more code with the non-test side here
    // without making things complex???

    static {
        // just like bootstrap, initialize natives, then SM
        Bootstrap.initializeNatives(true, true);
        // install security manager if requested
        if (systemPropertyAsBoolean("tests.security.manager", false)) {
            try {
                // initialize tmpdir the same exact way as bootstrap.
                Permissions perms = new Permissions();
                Security.addPath(perms, PathUtils.get(System.getProperty("java.io.tmpdir")), "read,readlink,write,delete");
                Policy.setPolicy(new ESPolicy(perms));
                System.setSecurityManager(new TestSecurityManager());
                Security.selfTest();
            } catch (Exception e) {
                throw new RuntimeException("unable to install test security manager", e);
            }
        }
    }

    // does nothing, just easy way to make sure the class is loaded.
    static void ensureInitialized() {}
}
