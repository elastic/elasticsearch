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
package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class NodeMetaDataTests extends ESTestCase {
    private Version randomVersion() {
        // VersionUtils.randomVersion() only returns known versions, which are necessarily no later than Version.CURRENT; however we want
        // also to consider our behaviour with all versions, so occasionally pick up a truly random version.
        return rarely() ? Version.fromId(randomInt()) : VersionUtils.randomVersion(random());
    }

    public void testEqualsHashcodeSerialization() {
        final Path tempDir = createTempDir();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new NodeMetaData(randomAlphaOfLength(10), randomVersion()),
            nodeMetaData -> {
                final long generation = NodeMetaData.FORMAT.writeAndCleanup(nodeMetaData, tempDir);
                final Tuple<NodeMetaData, Long> nodeMetaDataLongTuple
                    = NodeMetaData.FORMAT.loadLatestStateWithGeneration(logger, xContentRegistry(), tempDir);
                assertThat(nodeMetaDataLongTuple.v2(), equalTo(generation));
                return nodeMetaDataLongTuple.v1();
            }, nodeMetaData -> {
                if (randomBoolean()) {
                    return new NodeMetaData(randomAlphaOfLength(21 - nodeMetaData.nodeId().length()), nodeMetaData.nodeVersion());
                } else {
                    return new NodeMetaData(nodeMetaData.nodeId(), randomValueOtherThan(nodeMetaData.nodeVersion(), this::randomVersion));
                }
            });
    }

    public void testReadsFormatWithoutVersion() throws IOException {
        // the behaviour tested here is only appropriate if the current version is compatible with versions 7 and earlier
        assertTrue(Version.CURRENT.minimumIndexCompatibilityVersion().onOrBefore(Version.V_7_0_0));
        // when the current version is incompatible with version 7, the behaviour should change to reject files like the given resource
        // which do not have the version field

        final Path tempDir = createTempDir();
        final Path stateDir = Files.createDirectory(tempDir.resolve(MetaDataStateFormat.STATE_DIR_NAME));
        final InputStream resource = this.getClass().getResourceAsStream("testReadsFormatWithoutVersion.binary");
        assertThat(resource, notNullValue());
        Files.copy(resource, stateDir.resolve(NodeMetaData.FORMAT.getStateFileName(between(0, Integer.MAX_VALUE))));
        final NodeMetaData nodeMetaData = NodeMetaData.FORMAT.loadLatestState(logger, xContentRegistry(), tempDir);
        assertThat(nodeMetaData.nodeId(), equalTo("y6VUVMSaStO4Tz-B5BxcOw"));
        assertThat(nodeMetaData.nodeVersion(), equalTo(Version.V_EMPTY));
    }

    public void testUpgradesLegitimateVersions() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetaData nodeMetaData = new NodeMetaData(nodeId,
            randomValueOtherThanMany(v -> v.after(Version.CURRENT) || v.before(Version.CURRENT.minimumIndexCompatibilityVersion()),
                this::randomVersion)).upgradeToCurrentVersion();
        assertThat(nodeMetaData.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetaData.nodeId(), equalTo(nodeId));
    }

    public void testUpgradesMissingVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetaData nodeMetaData = new NodeMetaData(nodeId, Version.V_EMPTY).upgradeToCurrentVersion();
        assertThat(nodeMetaData.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetaData.nodeId(), equalTo(nodeId));
    }

    public void testDoesNotUpgradeFutureVersion() {
        final IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> new NodeMetaData(randomAlphaOfLength(10), tooNewVersion())
                .upgradeToCurrentVersion());
        assertThat(illegalStateException.getMessage(),
            allOf(startsWith("cannot downgrade a node from version ["), endsWith("] to version [" + Version.CURRENT + "]")));
    }

    public void testDoesNotUpgradeAncientVersion() {
        final IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> new NodeMetaData(randomAlphaOfLength(10), tooOldVersion()).upgradeToCurrentVersion());
        assertThat(illegalStateException.getMessage(),
            allOf(startsWith("cannot upgrade a node from version ["), endsWith("] directly to version [" + Version.CURRENT + "]")));
    }

    public static Version tooNewVersion() {
        return Version.fromId(between(Version.CURRENT.id + 1, 99999999));
    }

    public static Version tooOldVersion() {
        return Version.fromId(between(1, Version.CURRENT.minimumIndexCompatibilityVersion().id - 1));
    }
}
