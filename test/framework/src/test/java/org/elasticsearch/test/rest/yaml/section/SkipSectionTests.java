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

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

public class SkipSectionTests extends ESTestCase {

    public void testSkip() {
        SkipSection section = new SkipSection("2.0.0 - 2.1.0", randomBoolean() ? Collections.emptyList() :
        Arrays.asList("warnings"), "foobar");
        assertFalse(section.skip(Version.CURRENT));
        assertTrue(section.skip(Version.V_2_0_0));
        section = new SkipSection(randomBoolean() ? null : "2.0.0 - 2.1.0", Arrays.asList("boom"), "foobar");
        assertTrue(section.skip(Version.CURRENT));
    }

    public void testMessage() {
        SkipSection section = new SkipSection("2.0.0 - 2.1.0", Arrays.asList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(null, Arrays.asList("warnings"), "foobar");
        assertEquals("[FOOBAR] skipped, reason: [foobar] unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
        section = new SkipSection(null, Arrays.asList("warnings"), null);
        assertEquals("[FOOBAR] skipped, unsupported features [warnings]", section.getSkipMessage("FOOBAR"));
    }
}
