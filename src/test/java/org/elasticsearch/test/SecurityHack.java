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

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;

/** 
 * Installs test security manager (ensures it happens regardless of which
 * test case happens to be first, test ordering, etc). 
 * <p>
 * Note that this is BS, this should be done by the jvm (by passing -Djava.security.manager).
 * turning it on/off needs to be the role of maven, not this stuff.
 */
class SecurityHack {

    static {
        // for IDEs, we check that security.policy is set
        if (systemPropertyAsBoolean("tests.security.manager", true) && 
                System.getProperty("java.security.policy") != null) {
            System.setSecurityManager(new TestSecurityManager());
        }
    }

    // does nothing, just easy way to make sure the class is loaded.
    static void ensureInitialized() {}
}
