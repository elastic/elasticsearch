/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.MetadataStateFormat;
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

public class NodeMetadataTests extends ESTestCase {
    private Version randomVersion() {
        // VersionUtils.randomVersion() only returns known versions, which are necessarily no later than Version.CURRENT; however we want
        // also to consider our behaviour with all versions, so occasionally pick up a truly random version.
        return rarely() ? Version.fromId(randomInt()) : VersionUtils.randomVersion(random());
    }

    public void testEqualsHashcodeSerialization() {
        final Path tempDir = createTempDir();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new NodeMetadata(randomAlphaOfLength(10), randomVersion()),
            nodeMetadata -> {
                final long generation = NodeMetadata.FORMAT.writeAndCleanup(nodeMetadata, tempDir);
                final Tuple<NodeMetadata, Long> nodeMetadataLongTuple
                    = NodeMetadata.FORMAT.loadLatestStateWithGeneration(logger, xContentRegistry(), tempDir);
                assertThat(nodeMetadataLongTuple.v2(), equalTo(generation));
                return nodeMetadataLongTuple.v1();
            }, nodeMetadata -> {
                if (randomBoolean()) {
                    return new NodeMetadata(randomAlphaOfLength(21 - nodeMetadata.nodeId().length()), nodeMetadata.nodeVersion());
                } else {
                    return new NodeMetadata(nodeMetadata.nodeId(), randomValueOtherThan(nodeMetadata.nodeVersion(), this::randomVersion));
                }
            });
    }

    public void testReadsFormatWithoutVersion() throws IOException {
        // the behaviour tested here is only appropriate if the current version is compatible with versions 7 and earlier
        assertTrue(Version.CURRENT.minimumIndexCompatibilityVersion().onOrBefore(Version.V_7_0_0));
        // when the current version is incompatible with version 7, the behaviour should change to reject files like the given resource
        // which do not have the version field

        final Path tempDir = createTempDir();
        final Path stateDir = Files.createDirectory(tempDir.resolve(MetadataStateFormat.STATE_DIR_NAME));
        final InputStream resource = this.getClass().getResourceAsStream("testReadsFormatWithoutVersion.binary");
        assertThat(resource, notNullValue());
        Files.copy(resource, stateDir.resolve(NodeMetadata.FORMAT.getStateFileName(between(0, Integer.MAX_VALUE))));
        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempDir);
        assertThat(nodeMetadata.nodeId(), equalTo("y6VUVMSaStO4Tz-B5BxcOw"));
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.V_EMPTY));
    }

    public void testUpgradesLegitimateVersions() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId,
            randomValueOtherThanMany(v -> v.after(Version.CURRENT) || v.before(Version.CURRENT.minimumIndexCompatibilityVersion()),
                this::randomVersion)).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    public void testUpgradesMissingVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId, Version.V_EMPTY).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    public void testDoesNotUpgradeFutureVersion() {
        final IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooNewVersion())
                .upgradeToCurrentVersion());
        assertThat(illegalStateException.getMessage(),
            allOf(startsWith("cannot downgrade a node from version ["), endsWith("] to version [" + Version.CURRENT + "]")));
    }

    public void testDoesNotUpgradeAncientVersion() {
        final IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooOldVersion()).upgradeToCurrentVersion());
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
