/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Tuple;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * Represents the version of the wire protocol used to communicate between a pair of ES nodes.
 *
 * <h2>Defining transport versions</h2>
 * Transport versions can be defined anywhere, including in plugins/modules. Defining a new transport version is done via the
 * {@link TransportVersion#fromName(String)} method, for example:
 * <pre>
 *     private static final TransportVersion MY_NEW_TRANSPORT_VERSION = TransportVersion.fromName("my-new-transport-version");
 * </pre>
 *
 * Names must be logically unique. The same name must not be used to represent two transport versions with differing behavior. However,
 * the same name may be used to define the same constant at multiple use-sites. Alternatively, a single constant can be shared across
 * multiple use sites.
 *
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link VersionsHolder#MINIMUM_COMPATIBLE} field. Previously, this was dynamically
 * calculated from the major/minor versions of {@link Version}, but {@code TransportVersion} does not have separate major/minor version
 * numbers. So the minimum compatible version is hard-coded as the transport version used by the highest minor release of the previous
 * major version. {@link VersionsHolder#MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <p>
 * The earliest CCS compatible version is hardcoded at {@link VersionsHolder#MINIMUM_CCS_VERSION}, as the transport version used by the
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

    @Deprecated(forRemoval = true)
    public boolean after(TransportVersion version) {
        return version.id < id;
    }

    @Deprecated(forRemoval = true)
    public boolean onOrAfter(TransportVersion version) {
        throw new UnsupportedOperationException("use TransportVersion.supports(...) instead");
    }

    @Deprecated(forRemoval = true)
    public boolean between(TransportVersion lowerInclusive, TransportVersion upperExclusive) {
        throw new UnsupportedOperationException("use TransportVersion.supports(...) && TransportVersion.supports(...) == false instead");
    }

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
     * This method takes in the parameter {@code upperBound} which is the highest transport version id
     * that will be loaded by this node.
     */
    static TransportVersion fromBufferedReader(
        String component,
        String path,
        boolean nameInFile,
        boolean isNamed,
        BufferedReader bufferedReader,
        Integer upperBound
    ) {
        try {
            String line;
            do {
                line = bufferedReader.readLine();
            } while (line.replaceAll("\\s+", "").startsWith("#"));
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
            String name = null;
            if (isNamed) {
                if (nameInFile) {
                    name = parts[0];
                } else {
                    name = path.substring(path.lastIndexOf('/') + 1, path.length() - 4);
                }
            }
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
                if (ids.get(idIndex) > upperBound) {
                    break;
                }
                transportVersion = new TransportVersion(name, ids.get(idIndex), transportVersion);
            }
            return transportVersion;
        } catch (IOException ioe) {
            throw new UncheckedIOException("invalid transport version file format [" + toComponentPath(component, path) + "]", ioe);
        }
    }

    public static List<TransportVersion> collectFromResources(
        String component,
        String resourceRoot,
        Function<String, InputStream> resourceLoader,
        String upperBoundFileName
    ) {
        TransportVersion upperBound = parseFromBufferedReader(
            component,
            resourceRoot + "/upper_bounds/" + upperBoundFileName,
            resourceLoader,
            (c, p, br) -> fromBufferedReader(c, p, true, false, br, Integer.MAX_VALUE)
        );
        if (upperBound != null) {
            List<String> versionRelativePaths = parseFromBufferedReader(
                component,
                resourceRoot + "/definitions/manifest.txt",
                resourceLoader,
                (c, p, br) -> br.lines().filter(line -> line.isBlank() == false).toList()
            );
            if (versionRelativePaths != null) {
                List<TransportVersion> transportVersions = new ArrayList<>();
                for (String versionRelativePath : versionRelativePaths) {
                    TransportVersion transportVersion = parseFromBufferedReader(
                        component,
                        resourceRoot + "/definitions/" + versionRelativePath,
                        resourceLoader,
                        (c, p, br) -> fromBufferedReader(c, p, false, versionRelativePath.startsWith("referable/"), br, upperBound.id())
                    );
                    if (transportVersion != null) {
                        transportVersions.add(transportVersion);
                    }
                }
                return transportVersions;
            }
        }
        return List.of();
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
     * This will only return the latest known referable transport version for a given name and not its
     * patch versions. Patch versions are constructed as a linked list internally and may be found by
     * cycling through them in a loop using {@link TransportVersion#nextPatchVersion()}.
     */
    public static TransportVersion fromName(String name) {
        TransportVersion known = VersionsHolder.ALL_VERSIONS_BY_NAME.get(name);
        if (known == null) {
            LevenshteinDistance ld = new LevenshteinDistance();
            List<Tuple<Float, String>> scoredNames = new ArrayList<>();
            for (String key : VersionsHolder.ALL_VERSIONS_BY_NAME.keySet()) {
                float distance = ld.getDistance(name, key);
                if (distance > 0.7f) {
                    scoredNames.add(new Tuple<>(distance, key));
                }
            }
            StringBuilder message = new StringBuilder("Unknown transport version [");
            message.append(name);
            message.append("].");
            if (scoredNames.isEmpty() == false) {
                List<String> names = scoredNames.stream().map(Tuple::v2).toList();
                message.append(" Did you mean ");
                message.append(names);
                message.append("?");
            }
            message.append(" If this is a new transport version, run './gradlew generateTransportVersion'.");
            throw new IllegalStateException(message.toString());
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
        return version.id >= VersionsHolder.MINIMUM_COMPATIBLE.id;
    }

    /**
     * Reference to the most recent transport version.
     * This should be the transport version with the highest id.
     */
    public static TransportVersion current() {
        return VersionsHolder.CURRENT;
    }

    /**
     * Sentinel value for lowest possible transport version
     */
    public static TransportVersion zero() {
        return VersionsHolder.ZERO;
    }

    /**
     * Reference to the earliest compatible transport version to this version of the codebase.
     * This should be the transport version used by the highest minor version of the previous major.
     */
    public static TransportVersion minimumCompatible() {
        return VersionsHolder.MINIMUM_COMPATIBLE;
    }

    /**
     * Reference to the minimum transport version that can be used with CCS.
     * This should be the transport version used by the previous minor release.
     */
    public static TransportVersion minimumCCSVersion() {
        return VersionsHolder.MINIMUM_CCS_VERSION;
    }

    /**
     * Sorted list of all defined transport versions
     */
    public static List<TransportVersion> getAllVersions() {
        return VersionsHolder.ALL_VERSIONS;
    }

    /**
     * @return whether this is a known {@link TransportVersion}, i.e. one declared via {@link #fromName(String)}. Other versions may exist
     *         in the wild (they're sent over the wire by numeric ID) but we don't know how to communicate using such versions.
     */
    public boolean isKnown() {
        return VersionsHolder.ALL_VERSIONS_BY_ID.containsKey(id);
    }

    /**
     * @return the newest known {@link TransportVersion} which is no older than this instance. Returns {@link VersionsHolder#ZERO} if
     *         there are no such versions.
     */
    public TransportVersion bestKnownVersion() {
        if (isKnown()) {
            return this;
        }
        TransportVersion bestSoFar = VersionsHolder.ZERO;
        for (final var knownVersion : VersionsHolder.ALL_VERSIONS_BY_ID.values()) {
            if (knownVersion.id > bestSoFar.id && knownVersion.id < this.id) {
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
     */
    public boolean isPatchFrom(TransportVersion version) {
        return id >= version.id && id < version.id + 100 - (version.id % 100);
    }

    /**
     * Returns {@code true} if this version is a patch version. Transport versions are generally monotoic, that is, when comparing
     * transport versions via {@link #compareTo(VersionId)} a later version is also temporally "newer". This, however, is not the case
     * for patch versions, as they can be introduced at any time. There may be cases where this distinction is important, in which case
     * this method can be used to determine if a version is a patch, and therefore, may actually be temporally newer than "later" versions.
     *
     * @return whether this version is a patch version.
     */
    public boolean isPatchVersion() {
        return id % 100 != 0;
    }

    /**
     * Supports is used to determine if a named transport version is supported
     * by a caller transport version. This will check both the latest id
     * and all of its patch ids for compatibility. This replaces the pattern
     * of {@code wireTV.onOrAfter(TV_FEATURE) || wireTV.isPatchFrom(TV_FEATURE_BACKPORT) || ...}
     * for unnamed transport versions with {@code wireTV.supports(TV_FEATURE)} for named
     * transport versions (since referable versions know about their own patch versions).
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
        if (id >= version.id) {
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
     * This class holds various data structures for loading transport versions
     */
    private static class VersionsHolder {

        private static final List<TransportVersion> ALL_VERSIONS;
        private static final Map<Integer, TransportVersion> ALL_VERSIONS_BY_ID;
        private static final Map<String, TransportVersion> ALL_VERSIONS_BY_NAME;
        private static final IntFunction<String> VERSION_LOOKUP_BY_RELEASE;

        private static final TransportVersion CURRENT;
        private static final TransportVersion ZERO;
        private static final TransportVersion MINIMUM_COMPATIBLE;
        private static final TransportVersion MINIMUM_CCS_VERSION;

        static {
            // collect all the transport versions from server and es modules/plugins (defined in server)
            List<TransportVersion> allVersions = new ArrayList<>();
            List<TransportVersion> streamVersions = collectFromResources(
                "<server>",
                "/transport",
                TransportVersion.class::getResourceAsStream,
                Version.CURRENT.major + "." + Version.CURRENT.minor + ".csv"
            );
            Map<String, TransportVersion> allVersionsByName = streamVersions.stream()
                .filter(tv -> tv.name() != null)
                .collect(Collectors.toMap(TransportVersion::name, v -> v));
            addTransportVersions(streamVersions, allVersions).sort(TransportVersion::compareTo);

            // set version lookup by release before adding serverless versions
            // serverless versions should not affect release version
            VERSION_LOOKUP_BY_RELEASE = ReleaseVersions.generateVersionsLookup(
                "/org/elasticsearch/TransportVersions.csv",
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
            ZERO = new TransportVersion(0);
            MINIMUM_COMPATIBLE = loadConstant("minimum_compatible");
            MINIMUM_CCS_VERSION = loadConstant("minimum_ccs_version");
        }

        private static TransportVersion loadConstant(String name) {
            return parseFromBufferedReader(
                "<server>",
                "/transport/constants/" + name + ".csv",
                TransportVersion.class::getResourceAsStream,
                (c, p, br) -> fromBufferedReader(c, p, false, false, br, Integer.MAX_VALUE)
            );
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
