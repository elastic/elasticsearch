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
package org.elasticsearch.test.test;

import org.elasticsearch.Version;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.List;

public class VersionUtilsTests extends ElasticsearchTestCase {

    public void testAllVersionsSorted() {
        List<Version> allVersions = VersionUtils.allVersions();
        for (int i = 0, j = 1; j < allVersions.size(); ++i, ++j) {
            assertTrue(allVersions.get(i).before(allVersions.get(j)));
        }
    }
    
    public void testRandomVersionBetween() {
        // full range
        Version got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), Version.CURRENT);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), null, Version.CURRENT);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), null);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));

        // sub range
        got = VersionUtils.randomVersionBetween(random(), Version.V_0_90_12, Version.V_1_4_5);
        assertTrue(got.onOrAfter(Version.V_0_90_12));
        assertTrue(got.onOrBefore(Version.V_1_4_5));

        // unbounded lower
        got = VersionUtils.randomVersionBetween(random(), null, Version.V_1_4_5);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.V_1_4_5));
        got = VersionUtils.randomVersionBetween(random(), null, VersionUtils.allVersions().get(0));
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(VersionUtils.allVersions().get(0)));

        // unbounded upper
        got = VersionUtils.randomVersionBetween(random(), Version.V_0_90_12, null);
        assertTrue(got.onOrAfter(Version.V_0_90_12));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getPreviousVersion(), null);
        assertTrue(got.onOrAfter(VersionUtils.getPreviousVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));
        
        // range of one
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), VersionUtils.getFirstVersion());
        assertEquals(got, VersionUtils.getFirstVersion());
        got = VersionUtils.randomVersionBetween(random(), Version.CURRENT, Version.CURRENT);
        assertEquals(got, Version.CURRENT);
        got = VersionUtils.randomVersionBetween(random(), Version.V_1_2_4, Version.V_1_2_4);
        assertEquals(got, Version.V_1_2_4);
        
        // implicit range of one
        got = VersionUtils.randomVersionBetween(random(), null, VersionUtils.getFirstVersion());
        assertEquals(got, VersionUtils.getFirstVersion());
        got = VersionUtils.randomVersionBetween(random(), Version.CURRENT, null);
        assertEquals(got, Version.CURRENT);
    }
}
