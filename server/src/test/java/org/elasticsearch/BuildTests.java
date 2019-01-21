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
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

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

    public void testIsProduction() {
        Build build = new Build(
            Build.CURRENT.flavor(), Build.CURRENT.type(), Build.CURRENT.shortHash(), Build.CURRENT.date(),
            Build.CURRENT.isSnapshot(), Math.abs(randomInt()) + "." + Math.abs(randomInt()) + "." + Math.abs(randomInt())
        );
        assertTrue(build.getQualifiedVersion(), build.isProductionRelease());

        assertFalse(new Build(
            Build.CURRENT.flavor(), Build.CURRENT.type(), Build.CURRENT.shortHash(), Build.CURRENT.date(),
            Build.CURRENT.isSnapshot(), "7.0.0-alpha1"
        ).isProductionRelease());

        assertFalse(new Build(
            Build.CURRENT.flavor(), Build.CURRENT.type(), Build.CURRENT.shortHash(), Build.CURRENT.date(),
            Build.CURRENT.isSnapshot(), "7.0.0-alpha1-SNAPSHOT"
        ).isProductionRelease());

        assertFalse(new Build(
            Build.CURRENT.flavor(), Build.CURRENT.type(), Build.CURRENT.shortHash(), Build.CURRENT.date(),
            Build.CURRENT.isSnapshot(), "7.0.0-SNAPSHOT"
        ).isProductionRelease());

        assertFalse(new Build(
            Build.CURRENT.flavor(), Build.CURRENT.type(), Build.CURRENT.shortHash(), Build.CURRENT.date(),
            Build.CURRENT.isSnapshot(), "Unknown"
        ).isProductionRelease());
    }

    public void testEqualsAndHashCode() {
        Build build = Build.CURRENT;

        Build another = new Build(
            build.flavor(), build.type(), build.shortHash(), build.date(), build.isSnapshot(), build.getQualifiedVersion()
        );
        assertEquals(build, another);
        assertEquals(build.hashCode(), another.hashCode());

        final Set<Build.Flavor> otherFlavors =
                Arrays.stream(Build.Flavor.values()).filter(f -> !f.equals(build.flavor())).collect(Collectors.toSet());
        final Build.Flavor otherFlavor = randomFrom(otherFlavors);
        Build differentFlavor = new Build(
            otherFlavor, build.type(), build.shortHash(), build.date(), build.isSnapshot(), build.getQualifiedVersion()
        );
        assertNotEquals(build, differentFlavor);

        final Set<Build.Type> otherTypes =
                Arrays.stream(Build.Type.values()).filter(f -> !f.equals(build.type())).collect(Collectors.toSet());
        final Build.Type otherType = randomFrom(otherTypes);
        Build differentType = new Build(
            build.flavor(), otherType, build.shortHash(), build.date(), build.isSnapshot(), build.getQualifiedVersion()
        );
        assertNotEquals(build, differentType);

        Build differentHash = new Build(
            build.flavor(), build.type(), randomAlphaOfLengthBetween(3, 10), build.date(), build.isSnapshot(),
            build.getQualifiedVersion()
        );
        assertNotEquals(build, differentHash);

        Build differentDate = new Build(
            build.flavor(), build.type(), build.shortHash(), "1970-01-01", build.isSnapshot(), build.getQualifiedVersion()
        );
        assertNotEquals(build, differentDate);

        Build differentSnapshot = new Build(
            build.flavor(), build.type(), build.shortHash(), build.date(), !build.isSnapshot(), build.getQualifiedVersion()
        );
        assertNotEquals(build, differentSnapshot);

        Build differentVersion = new Build(
            build.flavor(), build.type(), build.shortHash(), build.date(), build.isSnapshot(), "1.2.3"
        );
        assertNotEquals(build, differentVersion);
    }
}
