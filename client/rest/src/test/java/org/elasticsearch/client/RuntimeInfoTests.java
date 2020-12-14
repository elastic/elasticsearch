/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import static org.junit.Assert.assertEquals;

public class RuntimeInfoTests extends RestClientTestCase {

    public void testStripPatchRevision() {

        assertEquals("1.2", RuntimeInfo.stripPatchRevision("1.2.3"));
        assertEquals("1.2", RuntimeInfo.stripPatchRevision("1.2"));
        assertEquals("1", RuntimeInfo.stripPatchRevision("1"));

        // Test language-specific representations of non-final versions

        // Kotlin
        assertEquals("1.4", RuntimeInfo.stripPatchRevision("1.4.20-M2"));
        assertEquals("1.3", RuntimeInfo.stripPatchRevision("1.3.0-rc-198"));

        // Scala
        assertEquals("2.13", RuntimeInfo.stripPatchRevision("2.13.0-RC3"));
        assertEquals("2.13", RuntimeInfo.stripPatchRevision("2.13.0-M5-6e0cba7"));
        assertEquals("2.11", RuntimeInfo.stripPatchRevision("2.11.8-18269ea"));

        // Clojure
        assertEquals("1.10", RuntimeInfo.stripPatchRevision("1.10.2-alpha1"));

        // Groovy
        assertEquals("3.0", RuntimeInfo.stripPatchRevision("3.0.0-rc-3"));
    }
}
