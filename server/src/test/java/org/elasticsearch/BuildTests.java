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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

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

        Build another = new Build(build.flavor(), build.type(), build.shortHash(), build.date(), build.isSnapshot());
        assertEquals(build, another);
        assertEquals(build.hashCode(), another.hashCode());

        final Set<Build.Flavor> otherFlavors =
                Arrays.stream(Build.Flavor.values()).filter(f -> !f.equals(build.flavor())).collect(Collectors.toSet());
        final Build.Flavor otherFlavor = randomFrom(otherFlavors);
        Build differentFlavor = new Build(otherFlavor, build.type(), build.shortHash(), build.date(), build.isSnapshot());
        assertNotEquals(build, differentFlavor);

        final Set<Build.Type> otherTypes =
                Arrays.stream(Build.Type.values()).filter(f -> !f.equals(build.type())).collect(Collectors.toSet());
        final Build.Type otherType = randomFrom(otherTypes);
        Build differentType = new Build(build.flavor(), otherType, build.shortHash(), build.date(), build.isSnapshot());
        assertNotEquals(build, differentType);

        Build differentHash = new Build(build.flavor(), build.type(), randomAlphaOfLengthBetween(3, 10), build.date(), build.isSnapshot());
        assertNotEquals(build, differentHash);

        Build differentDate = new Build(build.flavor(), build.type(), build.shortHash(), "1970-01-01", build.isSnapshot());
        assertNotEquals(build, differentDate);

        Build differentSnapshot = new Build(build.flavor(), build.type(), build.shortHash(), build.date(), !build.isSnapshot());
        assertNotEquals(build, differentSnapshot);
    }

    private static class WriteableBuild implements Writeable {
        private final Build build;

        WriteableBuild(StreamInput in) throws IOException {
            build = Build.readBuild(in);
        }

        WriteableBuild(Build build) {
            this.build = build;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Build.writeBuild(build, out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WriteableBuild that = (WriteableBuild) o;
            return build.equals(that.build);
        }

        @Override
        public int hashCode() {
            return Objects.hash(build);
        }
    }

    private static String randomStringExcept(final String s) {
        return randomAlphaOfLength(13 - s.length());
    }

    public void testSerialization() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new WriteableBuild(new Build(
                randomFrom(Build.Flavor.values()), randomFrom(Build.Type.values()),
                randomAlphaOfLength(6), randomAlphaOfLength(6), randomBoolean())),
            b -> copyWriteable(b, writableRegistry(), WriteableBuild::new, Version.CURRENT),
            b -> {
                switch (randomIntBetween(1, 5)) {
                    case 1:
                        return new WriteableBuild(new Build(
                            randomValueOtherThan(b.build.flavor(), () -> randomFrom(Build.Flavor.values())), b.build.type(),
                            b.build.shortHash(), b.build.date(), b.build.isSnapshot()));
                    case 2:
                        return new WriteableBuild(new Build(b.build.flavor(),
                            randomValueOtherThan(b.build.type(), () -> randomFrom(Build.Type.values())),
                            b.build.shortHash(), b.build.date(), b.build.isSnapshot()));
                    case 3:
                        return new WriteableBuild(new Build(b.build.flavor(), b.build.type(),
                            randomStringExcept(b.build.shortHash()), b.build.date(), b.build.isSnapshot()));
                    case 4:
                        return new WriteableBuild(new Build(b.build.flavor(), b.build.type(),
                            b.build.shortHash(), randomStringExcept(b.build.date()), b.build.isSnapshot()));
                    case 5:
                        return new WriteableBuild(new Build(b.build.flavor(), b.build.type(),
                            b.build.shortHash(), b.build.date(), b.build.isSnapshot() == false));
                }
                throw new AssertionError();
            });
    }

    public void testSerializationBWC() throws IOException {
        final WriteableBuild dockerBuild = new WriteableBuild(new Build(randomFrom(Build.Flavor.values()), Build.Type.DOCKER,
            randomAlphaOfLength(6), randomAlphaOfLength(6), randomBoolean()));

        final List<Version> versions = Version.getDeclaredVersions(Version.class);
        final Version pre63Version = randomFrom(versions.stream().filter(v -> v.before(Version.V_6_3_0)).collect(Collectors.toList()));
        final Version post63Pre67Version = randomFrom(versions.stream()
            .filter(v -> v.onOrAfter(Version.V_6_3_0) && v.before(Version.V_6_7_0)).collect(Collectors.toList()));
        final Version post67Version = randomFrom(versions.stream().filter(v -> v.onOrAfter(Version.V_6_7_0)).collect(Collectors.toList()));

        final WriteableBuild pre63 = copyWriteable(dockerBuild, writableRegistry(), WriteableBuild::new, pre63Version);
        final WriteableBuild post63pre67 = copyWriteable(dockerBuild, writableRegistry(), WriteableBuild::new, post63Pre67Version);
        final WriteableBuild post67 = copyWriteable(dockerBuild, writableRegistry(), WriteableBuild::new, post67Version);

        assertThat(pre63.build.flavor(), equalTo(Build.Flavor.OSS));
        assertThat(post63pre67.build.flavor(), equalTo(dockerBuild.build.flavor()));
        assertThat(post67.build.flavor(), equalTo(dockerBuild.build.flavor()));

        assertThat(pre63.build.type(), equalTo(Build.Type.UNKNOWN));
        assertThat(post63pre67.build.type(), equalTo(Build.Type.TAR));
        assertThat(post67.build.type(), equalTo(dockerBuild.build.type()));
    }
}
