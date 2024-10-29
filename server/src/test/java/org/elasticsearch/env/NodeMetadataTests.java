/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.env;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

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
    // (Index)VersionUtils.randomVersion() only returns known versions, which are necessarily no later than (Index)Version.CURRENT;
    // however we want to also consider our behaviour with all versions, so occasionally pick up a truly random version.
    private Version randomVersion() {
        return rarely() ? Version.fromId(randomNonNegativeInt()) : VersionUtils.randomVersion(random());
    }

    private BuildVersion randomBuildVersion() {
        return BuildVersion.fromVersionId(randomVersion().id());
    }

    private IndexVersion randomIndexVersion() {
        return rarely() ? IndexVersion.fromId(randomInt()) : IndexVersionUtils.randomVersion(random());
    }

    public void testEqualsHashcodeSerialization() {
        final Path tempDir = createTempDir();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new NodeMetadata(randomAlphaOfLength(10), randomBuildVersion(), randomIndexVersion()),
            nodeMetadata -> {
                final long generation = NodeMetadata.FORMAT.writeAndCleanup(nodeMetadata, tempDir);
                final Tuple<NodeMetadata, Long> nodeMetadataLongTuple = NodeMetadata.FORMAT.loadLatestStateWithGeneration(
                    logger,
                    xContentRegistry(),
                    tempDir
                );
                assertThat(nodeMetadataLongTuple.v2(), equalTo(generation));
                return nodeMetadataLongTuple.v1();
            },
            nodeMetadata -> switch (randomInt(3)) {
                case 0 -> new NodeMetadata(
                    randomAlphaOfLength(21 - nodeMetadata.nodeId().length()),
                    nodeMetadata.nodeVersion(),
                    nodeMetadata.oldestIndexVersion()
                );
                case 1 -> new NodeMetadata(
                    nodeMetadata.nodeId(),
                    randomValueOtherThan(nodeMetadata.nodeVersion(), this::randomBuildVersion),
                    nodeMetadata.oldestIndexVersion()
                );
                default -> new NodeMetadata(
                    nodeMetadata.nodeId(),
                    nodeMetadata.nodeVersion(),
                    randomValueOtherThan(nodeMetadata.oldestIndexVersion(), this::randomIndexVersion)
                );
            }
        );
    }

    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA)
    @AwaitsFix(bugUrl = "as mentioned in the comment below, the behavior here is changing for 9.0 so this test needs updating")
    public void testReadsFormatWithoutVersion() throws IOException {
        // the behaviour tested here is only appropriate if the current version is compatible with versions 7 and earlier
        assertTrue(IndexVersions.MINIMUM_COMPATIBLE.onOrBefore(IndexVersions.V_7_0_0));
        // when the current version is incompatible with version 7, the behaviour should change to reject files like the given resource
        // which do not have the version field

        final Path tempDir = createTempDir();
        final Path stateDir = Files.createDirectory(tempDir.resolve(MetadataStateFormat.STATE_DIR_NAME));
        final InputStream resource = this.getClass().getResourceAsStream("testReadsFormatWithoutVersion.binary");
        assertThat(resource, notNullValue());
        Files.copy(resource, stateDir.resolve(NodeMetadata.FORMAT.getStateFileName(between(0, Integer.MAX_VALUE))));
        final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempDir);
        assertThat(nodeMetadata.nodeId(), equalTo("y6VUVMSaStO4Tz-B5BxcOw"));
        assertThat(nodeMetadata.nodeVersion(), equalTo(BuildVersion.fromVersionId(0)));
    }

    public void testUpgradesLegitimateVersions() {
        final String nodeId = randomAlphaOfLength(10);
        final NodeMetadata nodeMetadata = new NodeMetadata(
            nodeId,
            randomValueOtherThanMany(v -> v.isFutureVersion() || v.onOrAfterMinimumCompatible() == false, this::randomBuildVersion),
            IndexVersion.current()
        ).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(BuildVersion.current()));
        assertThat(nodeMetadata.nodeId(), equalTo(nodeId));
    }

    public void testUpgradesMissingVersion() {
        final String nodeId = randomAlphaOfLength(10);

        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(nodeId, BuildVersion.fromVersionId(0), IndexVersion.current()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            startsWith(
                "cannot upgrade a node from version [" + Version.V_EMPTY + "] directly to version [" + Build.current().version() + "]"
            )
        );
    }

    public void testDoesNotUpgradeFutureVersion() {
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooNewBuildVersion(), IndexVersion.current()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            allOf(startsWith("cannot downgrade a node from version ["), endsWith("] to version [" + Build.current().version() + "]"))
        );
    }

    public void testDoesNotUpgradeAncientVersion() {
        final IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> new NodeMetadata(randomAlphaOfLength(10), tooOldBuildVersion(), IndexVersion.current()).upgradeToCurrentVersion()
        );
        assertThat(
            illegalStateException.getMessage(),
            allOf(
                startsWith("cannot upgrade a node from version ["),
                endsWith(
                    "] directly to version ["
                        + Build.current().version()
                        + "], upgrade to version ["
                        + Build.current().minWireCompatVersion()
                        + "] first."
                )
            )
        );
    }

    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA)
    @AwaitsFix(bugUrl = "Needs to be updated for 9.0 version bump")
    public void testUpgradeMarksPreviousVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final Version version = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.V_8_0_0);
        final BuildVersion buildVersion = BuildVersion.fromVersionId(version.id());

        final NodeMetadata nodeMetadata = new NodeMetadata(nodeId, buildVersion, IndexVersion.current()).upgradeToCurrentVersion();
        assertThat(nodeMetadata.nodeVersion(), equalTo(BuildVersion.current()));
        assertThat(nodeMetadata.previousNodeVersion(), equalTo(buildVersion));
    }

    public static IndexVersion tooNewIndexVersion() {
        return IndexVersion.fromId(between(IndexVersion.current().id() + 1, 99999999));
    }

    public static BuildVersion tooNewBuildVersion() {
        return BuildVersion.fromVersionId(between(Version.CURRENT.id() + 1, 99999999));
    }

    public static BuildVersion tooOldBuildVersion() {
        return BuildVersion.fromVersionId(between(1, Version.CURRENT.minimumCompatibilityVersion().id - 1));
    }
}
