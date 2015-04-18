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
        int numReps = randomIntBetween(10, 20);
        while (numReps-- > 0) {
            Version v1 = VersionUtils.randomVersion(random());
            Version v2 = VersionUtils.randomVersion(random());
            if (v1.after(v2)) {
                Version tmp = v1;
                v1 = v2;
                v2 = tmp;
            }
            Version got = VersionUtils.randomVersionBetween(random(), v1, v2);
            assertTrue(got.onOrAfter(v1));
            assertTrue(got.onOrBefore(v2));

            got = VersionUtils.randomVersionBetween(random(), null, v2);
            assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
            assertTrue(got.onOrBefore(v2));

            got = VersionUtils.randomVersionBetween(random(), v1, null);
            assertTrue(got.onOrAfter(v1));
            assertTrue(got.onOrBefore(Version.CURRENT));

            got = VersionUtils.randomVersionBetween(random(), v1, v1);
            assertEquals(got, v1);
        }

    }
}
