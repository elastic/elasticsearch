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
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.IndexVersion;
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
import static org.hamcrest.Matchers.instanceOf;
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
        return rarely() ? IndexVersion.fromId(randomInt()) : IndexVersionUtils.randomVersion();
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

    public void testFailsToReadFormatWithoutVersion() throws IOException {
        final Path tempDir = createTempDir();
        final Path stateDir = Files.createDirectory(tempDir.resolve(MetadataStateFormat.STATE_DIR_NAME));
        final InputStream resource = this.getClass().getResourceAsStream("testReadsFormatWithoutVersion.binary");
        assertThat(resource, notNullValue());
        Files.copy(resource, stateDir.resolve(NodeMetadata.FORMAT.getStateFileName(between(0, Integer.MAX_VALUE))));

        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> NodeMetadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempDir)
        );
        Throwable rootCause = ex.getRootCause();
        assertThat(rootCause, instanceOf(IllegalStateException.class));
        assertThat("Node version is required in node metadata", equalTo(rootCause.getMessage()));
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

    public void testUpgradeMarksPreviousVersion() {
        final String nodeId = randomAlphaOfLength(10);
        final Version version = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.V_9_0_0);
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
