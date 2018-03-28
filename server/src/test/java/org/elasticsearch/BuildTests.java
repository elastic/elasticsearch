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

package org.elasticsearch;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class BuildTests extends ESTestCase {

    /** Asking for the jar metadata should not throw exception in tests, no matter how configured */
    public void testJarMetadata() throws IOException {
        URL url = Build.getElasticsearchCodeSourceLocation();
        // throws exception if does not exist, or we cannot access it
        try (InputStream ignored = FileSystemUtils.openFileURLStream(url)) {}
        // these should never be null
        assertNotNull(Build.CURRENT.date());
        assertNotNull(Build.CURRENT.shortHash());
    }

    public void testEqualsAndHashCode() {
        Build build = Build.CURRENT;
        Build another = new Build(build.shortHash(), build.date(), build.isSnapshot());
        assertEquals(build, another);
        assertEquals(build.hashCode(), another.hashCode());

        Build differentHash = new Build(randomAlphaOfLengthBetween(3, 10), build.date(), build.isSnapshot());
        assertNotEquals(build, differentHash);

        Build differentDate = new Build(build.shortHash(), "1970-01-01", build.isSnapshot());
        assertNotEquals(build, differentDate);

        Build differentSnapshot = new Build(build.shortHash(), build.date(), !build.isSnapshot());
        assertNotEquals(build, differentSnapshot);
    }
}
