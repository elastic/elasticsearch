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
package org.elasticsearch.test.transport;

import org.elasticsearch.test.ESTestCase;

public class MockTransportServiceTests extends ESTestCase {

    public void testBasePortGradle() {
        assumeTrue("requires running tests with Gradle", System.getProperty("tests.gradle") != null);
        // Gradle worker IDs are 1 based
        assertNotEquals(10300, MockTransportService.getBasePort());
    }

    public void testBasePortIDE() {
        assumeTrue("requires running tests without Gradle", System.getProperty("tests.gradle") == null);
        assertEquals(10300, MockTransportService.getBasePort());
    }

}
