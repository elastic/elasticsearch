/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.internal.VersionExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * Represents the version of the wire protocol used to communicate between a pair of ES nodes.
 * <p>
 * Note: We are currently transitioning to a file-based system to load and maintain transport versions. These file-based transport
 * versions are named and are referred to as named transport versions. Named transport versions also maintain a linked list of their
 * own patch versions to simplify transport version compatibility checks. Transport versions that continue to be loaded through
 * {@link TransportVersions} are referred to as unnamed transport versions. Unnamed transport versions will continue being used
 * over the wire as we only need the id for compatibility checks even against named transport versions. There are changes
 * throughout {@link TransportVersion} that are for this transition. For now, continue to use the existing system of adding unnamed
 * transport versions to {@link TransportVersions}.
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
public record TransportVersion(String name, int id, TransportVersion nextPatchVersion) implements VersionId<TransportVersion> {

    /**
     * Constructs an unnamed transport version.
     */
    public TransportVersion(int id) {
        this(null, id, null);
    }

    interface BufferedReaderParser<T> {
        T parse(String component, String path, BufferedReader bufferedReader);
    }

    static <T> T parseFromBufferedReader(
        String component,
        String path,
        Function<String, InputStream> nameToStream,
        BufferedReaderParser<T> parser
    ) {
        try (InputStream inputStream = nameToStream.apply(path)) {
            if (inputStream == null) {
                return null;
            }
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                return parser.parse(component, path, bufferedReader);
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException("parsing error [" + component + ":" + path + "]", ioe);
        }
    }

    /**
     * Constructs a named transport version along with its set of compatible patch versions from x-content.
     * This method takes in the parameter {@code latest} which is the highest valid transport version id
     * supported by this node. Versions newer than the current transport version id for this node are discarded.
     */
    public static TransportVersion fromBufferedReader(
        String component,
        String path,
        boolean nameInFile,
        BufferedReader bufferedReader,
        Integer latest
    ) {
        try {
            String line = bufferedReader.readLine();
            String[] parts = line.replaceAll("\\s+", "").split(",");
            String check;
            while ((check = bufferedReader.readLine()) != null) {
                if (check.replaceAll("\\s+", "").isEmpty() == false) {
                    throw new IllegalArgumentException("invalid transport version file format [" + toComponentPath(component, path) + "]");
                }
            }
            if (parts.length < (nameInFile ? 2 : 1)) {
                throw new IllegalStateException("invalid transport version file format [" + toComponentPath(component, path) + "]");
            }
            String name = nameInFile ? parts[0] : path.substring(path.lastIndexOf('/') + 1, path.length() - 4);
            List<Integer> ids = new ArrayList<>();
            for (int i = nameInFile ? 1 : 0; i < parts.length; ++i) {
                try {
                    ids.add(Integer.parseInt(parts[i]));
                } catch (NumberFormatException nfe) {
                    throw new IllegalStateException(
                        "invalid transport version file format [" + toComponentPath(component, path) + "]",
                        nfe
                    );
                }
            }
            TransportVersion transportVersion = null;
            for (int idIndex = ids.size() - 1; idIndex >= 0; --idIndex) {
                if (idIndex > 0 && ids.get(idIndex - 1) <= ids.get(idIndex)) {
                    throw new IllegalStateException("invalid transport version file format [" + toComponentPath(component, path) + "]");
                }
                if (ids.get(idIndex) > latest) {
                    break;
                }
                transportVersion = new TransportVersion(name, ids.get(idIndex), transportVersion);
            }
            return transportVersion;
        } catch (IOException ioe) {
            throw new UncheckedIOException("invalid transport version file format [" + toComponentPath(component, path) + "]", ioe);
        }
    }

    public static Map<String, TransportVersion> collectFromInputStreams(
        String component,
        Function<String, InputStream> nameToStream,
        String latestFileName
    ) {
        TransportVersion latest = parseFromBufferedReader(
            component,
            "/transport/latest/" + latestFileName,
            nameToStream,
            (c, p, br) -> fromBufferedReader(c, p, true, br, Integer.MAX_VALUE)
        );
        if (latest != null) {
            List<String> versionFilesNames = parseFromBufferedReader(
                component,
                "/transport/defined/manifest.txt",
                nameToStream,
                (c, p, br) -> br.lines().filter(line -> line.isBlank() == false).toList()
            );
            if (versionFilesNames != null) {
                Map<String, TransportVersion> transportVersions = new HashMap<>();
                for (String versionFileName : versionFilesNames) {
                    TransportVersion transportVersion = parseFromBufferedReader(
                        component,
                        "/transport/defined/" + versionFileName,
                        nameToStream,
                        (c, p, br) -> fromBufferedReader(c, p, false, br, latest.id())
                    );
                    if (transportVersion != null) {
                        transportVersions.put(versionFileName.substring(0, versionFileName.length() - 4), transportVersion);
                    }
                }
                return transportVersions;
            }
        }
        return Map.of();
    }

    private static String toComponentPath(String component, String path) {
        return component + ":" + path;
    }

    public static TransportVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    /**
     * Finds a {@code TransportVersion} by its id.
     * If a transport version with the specified ID does not exist,
     * this method creates and returns a new instance of {@code TransportVersion} with the specified ID.
     * The new instance is not registered in {@code TransportVersion.getAllVersions}.
     */
    public static TransportVersion fromId(int id) {
        TransportVersion known = VersionsHolder.ALL_VERSIONS_BY_ID.get(id);
        if (known != null) {
            return known;
        }
        // this is a version we don't otherwise know about - just create a placeholder
        return new TransportVersion(id);
    }

    /**
     * Finds a {@link TransportVersion} by its name. The parameter {@code name} must be a {@link String}
     * direct value or validation checks will fail. {@code TransportVersion.fromName("direct_value")}.
     * <p>
     * This will only return the latest known named transport version for a given name and not its
     * patch versions. Patch versions are constructed as a linked list internally and may be found by
     * cycling through them in a loop using {@link TransportVersion#nextPatchVersion()}.
     *
     */
    public static TransportVersion fromName(String name) {
        TransportVersion known = VersionsHolder.ALL_VERSIONS_BY_NAME.get(name);
        if (known == null) {
            throw new IllegalStateException("unknown transport version [" + name + "]");
        }
        return known;
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
        return VersionsHolder.CURRENT;
    }

    /**
     * Sorted list of all defined transport versions
     */
    public static List<TransportVersion> getAllVersions() {
        return VersionsHolder.ALL_VERSIONS;
    }

    /**
     * @return whether this is a known {@link TransportVersion}, i.e. one declared in {@link TransportVersions}. Other versions may exist
     *         in the wild (they're sent over the wire by numeric ID) but we don't know how to communicate using such versions.
     */
    public boolean isKnown() {
        return VersionsHolder.ALL_VERSIONS_BY_ID.containsKey(id);
    }

    /**
     * @return the newest known {@link TransportVersion} which is no older than this instance. Returns {@link TransportVersions#ZERO} if
     *         there are no such versions.
     */
    public TransportVersion bestKnownVersion() {
        if (isKnown()) {
            return this;
        }
        TransportVersion bestSoFar = TransportVersions.ZERO;
        for (final var knownVersion : VersionsHolder.ALL_VERSIONS_BY_ID.values()) {
            if (knownVersion.after(bestSoFar) && knownVersion.before(this)) {
                bestSoFar = knownVersion;
            }
        }
        return bestSoFar;
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
     * For example, {@code version.isPatchFrom(8_800_0_04)} will return the following for the given {@code version}:
     * <ul>
     *     <li>{@code 8_799_0_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
     *     <li>{@code 8_799_0_09.isPatchFrom(8_800_0_04)}: {@code false}</li>
     *     <li>{@code 8_800_0_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
     *     <li>{@code 8_800_0_03.isPatchFrom(8_800_0_04)}: {@code false}</li>
     *     <li>{@code 8_800_0_04.isPatchFrom(8_800_0_04)}: {@code true}</li>
     *     <li>{@code 8_800_0_49.isPatchFrom(8_800_0_04)}: {@code true}</li>
     *     <li>{@code 8_800_1_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
     *     <li>{@code 8_801_0_00.isPatchFrom(8_800_0_04)}: {@code false}</li>
     * </ul>
     */
    public boolean isPatchFrom(TransportVersion version) {
        return onOrAfter(version) && id < version.id + 100 - (version.id % 100);
    }

    /**
     * Supports is used to determine if a named transport version is supported
     * by a caller transport version. This will check both the latest id
     * and all of its patch ids for compatibility. This replaces the pattern
     * of {@code wireTV.onOrAfter(TV_FEATURE) || wireTV.isPatchFrom(TV_FEATURE_BACKPORT) || ...}
     * for unnamed transport versions with {@code wireTV.supports(TV_FEATURE)} for named
     * transport versions (since named versions know about their own patch versions).
     * <p>
     * The recommended use of this method is to declare a static final {@link TransportVersion}
     * as part of the file that it's used in. This constant is then used in conjunction with
     * this method to check transport version compatability.
     * <p>
     * An example:
     * {@code
     * public class ExampleClass {
     * ...
     *     TransportVersion TV_FEATURE = TransportVersion.fromName("tv_feature");
     *     ...
     *     public static ExampleClass readFrom(InputStream in) {
     *         ...
     *         if (in.getTransportVersion().supports(TV_FEATURE) {
     *             // read newer values
     *         }
     *         ...
     *     }
     *     ...
     *     public void writeTo(OutputStream out) {
     *         ...
     *         if (out.getTransportVersion().supports(TV_FEATURE) {
     *             // write newer values
     *         }
     *         ...
     *     }
     *     ...
     * }
     * }
     */
    public boolean supports(TransportVersion version) {
        if (onOrAfter(version)) {
            return true;
        }
        TransportVersion nextPatchVersion = version.nextPatchVersion;
        while (nextPatchVersion != null) {
            if (isPatchFrom(nextPatchVersion)) {
                return true;
            }
            nextPatchVersion = nextPatchVersion.nextPatchVersion;
        }
        return false;
    }

    /**
     * Returns a string representing the Elasticsearch release version of this transport version,
     * if applicable for this deployment, otherwise the raw version number.
     */
    public String toReleaseVersion() {
        return VersionsHolder.VERSION_LOOKUP_BY_RELEASE.apply(id);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TransportVersion that = (TransportVersion) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    /**
     * This class holds various data structures for looking up known transport versions both
     * named and unnamed. While we transition to named transport versions, this class will
     * load and merge unnamed transport versions from {@link TransportVersions} along with
     * named transport versions specified in a manifest file in resources.
     */
    private static class VersionsHolder {

        private static final List<TransportVersion> ALL_VERSIONS;
        private static final Map<Integer, TransportVersion> ALL_VERSIONS_BY_ID;
        private static final Map<String, TransportVersion> ALL_VERSIONS_BY_NAME;
        private static final IntFunction<String> VERSION_LOOKUP_BY_RELEASE;
        private static final TransportVersion CURRENT;

        static {
            // collect all the transport versions from server and es modules/plugins (defined in server)
            List<TransportVersion> allVersions = new ArrayList<>(TransportVersions.DEFINED_VERSIONS);
            Map<String, TransportVersion> allVersionsByName = collectFromInputStreams(
                "<server>",
                TransportVersion.class::getResourceAsStream,
                Version.CURRENT.major + "." + Version.CURRENT.minor + ".csv"
            );
            addTransportVersions(allVersionsByName.values(), allVersions).sort(TransportVersion::compareTo);

            // set version lookup by release before adding serverless versions
            // serverless versions should not affect release version
            VERSION_LOOKUP_BY_RELEASE = ReleaseVersions.generateVersionsLookup(
                TransportVersions.class,
                allVersions.get(allVersions.size() - 1).id()
            );

            // collect all the transport versions from serverless
            Collection<TransportVersion> extendedVersions = ExtensionLoader.loadSingleton(ServiceLoader.load(VersionExtension.class))
                .map(VersionExtension::getTransportVersions)
                .orElse(Collections.emptyList());
            addTransportVersions(extendedVersions, allVersions).sort(TransportVersion::compareTo);
            for (TransportVersion version : extendedVersions) {
                if (version.name() != null) {
                    allVersionsByName.put(version.name(), version);
                }
            }

            // set the transport version lookups
            ALL_VERSIONS = Collections.unmodifiableList(allVersions);
            ALL_VERSIONS_BY_ID = ALL_VERSIONS.stream().collect(Collectors.toUnmodifiableMap(TransportVersion::id, Function.identity()));
            ALL_VERSIONS_BY_NAME = Collections.unmodifiableMap(allVersionsByName);
            CURRENT = ALL_VERSIONS.getLast();
        }

        private static List<TransportVersion> addTransportVersions(Collection<TransportVersion> addFrom, List<TransportVersion> addTo) {
            for (TransportVersion transportVersion : addFrom) {
                addTo.add(transportVersion);
                TransportVersion patchVersion = transportVersion.nextPatchVersion();
                while (patchVersion != null) {
                    addTo.add(patchVersion);
                    patchVersion = patchVersion.nextPatchVersion();
                }
            }
            return addTo;
        }
    }
}
