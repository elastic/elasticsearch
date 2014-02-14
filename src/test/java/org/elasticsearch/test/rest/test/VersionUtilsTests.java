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
package org.elasticsearch.test.rest.test;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.test.rest.support.VersionUtils.parseVersionNumber;
import static org.elasticsearch.test.rest.support.VersionUtils.skipCurrentVersion;
import static org.hamcrest.Matchers.*;

public class VersionUtilsTests extends ElasticsearchTestCase {

    @Test
    public void testParseVersionNumber() {

        int[] versionNumber = parseVersionNumber("0.90.6");
        assertThat(versionNumber.length, equalTo(3));
        assertThat(versionNumber[0], equalTo(0));
        assertThat(versionNumber[1], equalTo(90));
        assertThat(versionNumber[2], equalTo(6));

        versionNumber = parseVersionNumber("0.90.999");
        assertThat(versionNumber.length, equalTo(3));
        assertThat(versionNumber[0], equalTo(0));
        assertThat(versionNumber[1], equalTo(90));
        assertThat(versionNumber[2], equalTo(999));

        versionNumber = parseVersionNumber("0.20.11");
        assertThat(versionNumber.length, equalTo(3));
        assertThat(versionNumber[0], equalTo(0));
        assertThat(versionNumber[1], equalTo(20));
        assertThat(versionNumber[2], equalTo(11));

        versionNumber = parseVersionNumber("1.0.0.Beta1");
        assertThat(versionNumber.length, equalTo(3));
        assertThat(versionNumber[0], equalTo(1));
        assertThat(versionNumber[1], equalTo(0));
        assertThat(versionNumber[2], equalTo(0));

        versionNumber = parseVersionNumber("1.0.0.RC1");
        assertThat(versionNumber.length, equalTo(3));
        assertThat(versionNumber[0], equalTo(1));
        assertThat(versionNumber[1], equalTo(0));
        assertThat(versionNumber[2], equalTo(0));

        versionNumber = parseVersionNumber("1.0.0");
        assertThat(versionNumber.length, equalTo(3));
        assertThat(versionNumber[0], equalTo(1));
        assertThat(versionNumber[1], equalTo(0));
        assertThat(versionNumber[2], equalTo(0));

        versionNumber = parseVersionNumber("1.0");
        assertThat(versionNumber.length, equalTo(2));
        assertThat(versionNumber[0], equalTo(1));
        assertThat(versionNumber[1], equalTo(0));

        versionNumber = parseVersionNumber("999");
        assertThat(versionNumber.length, equalTo(1));
        assertThat(versionNumber[0], equalTo(999));

        versionNumber = parseVersionNumber("0");
        assertThat(versionNumber.length, equalTo(1));
        assertThat(versionNumber[0], equalTo(0));

        try {
            parseVersionNumber("1.0.Beta1");
            fail("parseVersionNumber should have thrown an error");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("version is not a number"));
            assertThat(e.getCause(), instanceOf(NumberFormatException.class));
        }
    }

    @Test
    public void testSkipCurrentVersion() {
        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "0.90.2"), equalTo(true));
        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "0.90.3"), equalTo(true));
        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "0.90.6"), equalTo(true));

        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "0.20.10"), equalTo(false));
        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "0.90.1"), equalTo(false));
        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "0.90.7"), equalTo(false));
        assertThat(skipCurrentVersion("0.90.2 - 0.90.6", "1.0.0"), equalTo(false));

        assertThat(skipCurrentVersion(" 0.90.2  -  0.90.999 ", "0.90.15"), equalTo(true));
        assertThat(skipCurrentVersion("0.90.2 - 0.90.999", "1.0.0"), equalTo(false));

        assertThat(skipCurrentVersion("0  -  999", "0.90.15"), equalTo(true));
        assertThat(skipCurrentVersion("0  -  999", "0.20.1"), equalTo(true));
        assertThat(skipCurrentVersion("0  -  999", "1.0.0"), equalTo(true));

        assertThat(skipCurrentVersion("0.90.9  -  999", "1.0.0"), equalTo(true));
        assertThat(skipCurrentVersion("0.90.9  -  999", "0.90.8"), equalTo(false));

        try {
            assertThat(skipCurrentVersion("0.90.2 - 0.90.999 - 1.0.0", "1.0.0"), equalTo(false));
            fail("skipCurrentVersion should have thrown an error");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("too many skip versions found"));
        }

    }
}
