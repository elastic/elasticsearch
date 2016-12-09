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

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.singletonList;

public class ClientYamlTestSectionTests extends ESTestCase {
    public void testAddingDoWithoutWarningWithoutSkip() {
        int lineNumber = between(1, 10000);
        ClientYamlTestSection section = new ClientYamlTestSection("test");
        section.setSkipSection(SkipSection.EMPTY);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        section.addExecutableSection(doSection);
    }

    public void testAddingDoWithWarningWithSkip() {
        int lineNumber = between(1, 10000);
        ClientYamlTestSection section = new ClientYamlTestSection("test");
        section.setSkipSection(new SkipSection(null, singletonList("warnings"), null));
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeaders(singletonList("foo"));
        section.addExecutableSection(doSection);
    }

    public void testAddingDoWithWarningWithSkipButNotWarnings() {
        int lineNumber = between(1, 10000);
        ClientYamlTestSection section = new ClientYamlTestSection("test");
        section.setSkipSection(new SkipSection(null, singletonList("yaml"), null));
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeaders(singletonList("foo"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> section.addExecutableSection(doSection));
        assertEquals("Attempted to add a [do] with a [warnings] section without a corresponding [skip] so runners that do not support the"
                + " [warnings] section can skip the test at line [" + lineNumber + "]", e.getMessage());
    }

}
