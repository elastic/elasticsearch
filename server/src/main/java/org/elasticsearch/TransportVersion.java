/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.internal.VersionExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.IOException;
import java.util.ServiceLoader;

/**
 * Represents the version of the wire protocol used to communicate between a pair of ES nodes.
 * <p>
 * Prior to 8.8.0, the release {@link Version} was used everywhere. This class separates the wire protocol version from the release version.
 * <p>
 * Each transport version constant has an id number, which for versions prior to 8.9.0 is the same as the release version for backwards
 * compatibility. In 8.9.0 this is changed to an incrementing number, disconnected from the release version.
 * <p>
 * Each version constant has a unique id string. This is not actually used in the binary protocol, but is there to ensure each protocol
 * version is only added to the source file once. This string needs to be unique (normally a UUID, but can be any other unique nonempty
 * string). If two concurrent PRs add the same transport version, the different unique ids cause a git conflict, ensuring that the second PR
 * to be merged must be updated with the next free version first. Without the unique id string, git will happily merge the two versions
 * together, resulting in the same transport version being used across multiple commits, causing problems when you try to upgrade between
 * those two merged commits.
 *
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link TransportVersions#MINIMUM_COMPATIBLE} field. Previously, this was dynamically
 * calculated from the major/minor versions of {@link Version}, but {@code TransportVersion} does not have separate major/minor version
 * numbers. So the minimum compatible version is hard-coded as the transport version used by the highest minor release of the previous
 * major version. {@link TransportVersions#MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <p>
 * The earliest CCS compatible version is hardcoded at {@link TransportVersions#MINIMUM_CCS_VERSION}, as the transport version used by the
 * previous minor release. This should be updated appropriately whenever a minor release happens.
 *
 * <h2>Scope of usefulness of {@link TransportVersion}</h2>
 * {@link TransportVersion} is a property of the transport connection between a pair of nodes, and should not be used as an indication of
 * the version of any single node. The {@link TransportVersion} of a connection is negotiated between the nodes via some logic that is not
 * totally trivial, and may change in future. Any other places that might make decisions based on this version effectively have to reproduce
 * this negotiation logic, which would be fragile. If you need to make decisions based on the version of a single node, do so using a
 * different version value. If you need to know whether the cluster as a whole speaks a new enough {@link TransportVersion} to understand a
 * newly-added feature, use {@link org.elasticsearch.cluster.ClusterState#getMinTransportVersion}.
 */
public record TransportVersion(int id) implements VersionId<TransportVersion> {

    public static TransportVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static TransportVersion fromId(int id) {
        TransportVersion known = TransportVersions.VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }
        // this is a version we don't otherwise know about - just create a placeholder
        return new TransportVersion(id);
    }

    public static void writeVersion(TransportVersion version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     */
    public static TransportVersion min(TransportVersion version1, TransportVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     */
    public static TransportVersion max(TransportVersion version1, TransportVersion version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    /**
     * Returns {@code true} if the specified version is compatible with this running version of Elasticsearch.
     */
    public static boolean isCompatible(TransportVersion version) {
        return version.onOrAfter(TransportVersions.MINIMUM_COMPATIBLE);
    }

    /**
     * Reference to the most recent transport version.
     * This should be the transport version with the highest id.
     */
    public static TransportVersion current() {
        return CurrentHolder.CURRENT;
    }

    public static TransportVersion fromString(String str) {
        return TransportVersion.fromId(Integer.parseInt(str));
    }

    /**
     * Returns {@code true} if this version is a patch version at or after {@code version}.
     * <p>
     * This should not be used normally. It is used for matching patch versions of the same base version,
     * using the standard version number format specified in {@link TransportVersions}.
     * When a patch version of an existing transport version is created, {@code transportVersion.isPatchFrom(patchVersion)}
     * will match any transport version at or above {@code patchVersion} that is also of the same base version.
     * <p>
     * For example, {@code version.isPatchFrom(8_800_00_4)} will return the following for the given {@code version}:
     * <ul>
     *     <li>{@code 8_799_00_0.isPatchFrom(8_800_00_4)}: {@code false}</li>
     *     <li>{@code 8_799_00_9.isPatchFrom(8_800_00_4)}: {@code false}</li>
     *     <li>{@code 8_800_00_0.isPatchFrom(8_800_00_4)}: {@code false}</li>
     *     <li>{@code 8_800_00_3.isPatchFrom(8_800_00_4)}: {@code false}</li>
     *     <li>{@code 8_800_00_4.isPatchFrom(8_800_00_4)}: {@code true}</li>
     *     <li>{@code 8_800_00_9.isPatchFrom(8_800_00_4)}: {@code true}</li>
     *     <li>{@code 8_800_01_0.isPatchFrom(8_800_00_4)}: {@code false}</li>
     *     <li>{@code 8_801_00_0.isPatchFrom(8_800_00_4)}: {@code false}</li>
     * </ul>
     */
    public boolean isPatchFrom(TransportVersion version) {
        return onOrAfter(version) && id < version.id + 10 - (version.id % 10);
    }

    /**
     * Returns a string representing the Elasticsearch release version of this transport version,
     * if applicable for this deployment, otherwise the raw version number.
     */
    public String toReleaseVersion() {
        return TransportVersions.VERSION_LOOKUP.apply(id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    private static class CurrentHolder {
        private static final TransportVersion CURRENT = findCurrent();

        // finds the pluggable current version
        private static TransportVersion findCurrent() {
            var version = ExtensionLoader.loadSingleton(ServiceLoader.load(VersionExtension.class))
                .map(e -> e.getCurrentTransportVersion(TransportVersions.LATEST_DEFINED))
                .orElse(TransportVersions.LATEST_DEFINED);
            assert version.onOrAfter(TransportVersions.LATEST_DEFINED);
            return version;
        }
    }
}
