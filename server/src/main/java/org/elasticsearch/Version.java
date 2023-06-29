/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class Version implements Comparable<Version>, ToXContentFragment {
    /*
     * The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is alpha/beta/rc indicator AA
     * values below 25 are for alpha builder (since 5.0), and above 25 and below 50 are beta builds, and below 99 are RC builds, with 99
     * indicating a release the (internal) format of the id is there so we can easily do after/before checks on the id
     *
     * IMPORTANT: Unreleased vs. Released Versions
     *
     * All listed versions MUST be released versions, except the last major, the last minor and the last revison. ONLY those are required
     * as unreleased versions.
     *
     * Example: assume the last release is 7.3.0
     * The unreleased last major is the next major release, e.g. _8_.0.0
     * The unreleased last minor is the current major with a upped minor: 7._4_.0
     * The unreleased revision is the very release with a upped revision 7.3._1_
     */

    public static final int V_EMPTY_ID = 0;
    public static final Version V_EMPTY = new Version(V_EMPTY_ID, IndexVersion.ZERO);
    public static final Version V_7_0_0 = new Version(7_00_00_99, IndexVersion.V_7_0_0);
    public static final Version V_7_0_1 = new Version(7_00_01_99, IndexVersion.V_7_0_1);
    public static final Version V_7_1_0 = new Version(7_01_00_99, IndexVersion.V_7_1_0);
    public static final Version V_7_1_1 = new Version(7_01_01_99, IndexVersion.V_7_1_1);
    public static final Version V_7_2_0 = new Version(7_02_00_99, IndexVersion.V_7_2_0);
    public static final Version V_7_2_1 = new Version(7_02_01_99, IndexVersion.V_7_2_1);
    public static final Version V_7_3_0 = new Version(7_03_00_99, IndexVersion.V_7_3_0);
    public static final Version V_7_3_1 = new Version(7_03_01_99, IndexVersion.V_7_3_1);
    public static final Version V_7_3_2 = new Version(7_03_02_99, IndexVersion.V_7_3_2);
    public static final Version V_7_4_0 = new Version(7_04_00_99, IndexVersion.V_7_4_0);
    public static final Version V_7_4_1 = new Version(7_04_01_99, IndexVersion.V_7_4_1);
    public static final Version V_7_4_2 = new Version(7_04_02_99, IndexVersion.V_7_4_2);
    public static final Version V_7_5_0 = new Version(7_05_00_99, IndexVersion.V_7_5_0);
    public static final Version V_7_5_1 = new Version(7_05_01_99, IndexVersion.V_7_5_1);
    public static final Version V_7_5_2 = new Version(7_05_02_99, IndexVersion.V_7_5_2);
    public static final Version V_7_6_0 = new Version(7_06_00_99, IndexVersion.V_7_6_0);
    public static final Version V_7_6_1 = new Version(7_06_01_99, IndexVersion.V_7_6_1);
    public static final Version V_7_6_2 = new Version(7_06_02_99, IndexVersion.V_7_6_2);
    public static final Version V_7_7_0 = new Version(7_07_00_99, IndexVersion.V_7_7_0);
    public static final Version V_7_7_1 = new Version(7_07_01_99, IndexVersion.V_7_7_1);
    public static final Version V_7_8_0 = new Version(7_08_00_99, IndexVersion.V_7_8_0);
    public static final Version V_7_8_1 = new Version(7_08_01_99, IndexVersion.V_7_8_1);
    public static final Version V_7_9_0 = new Version(7_09_00_99, IndexVersion.V_7_9_0);
    public static final Version V_7_9_1 = new Version(7_09_01_99, IndexVersion.V_7_9_1);
    public static final Version V_7_9_2 = new Version(7_09_02_99, IndexVersion.V_7_9_2);
    public static final Version V_7_9_3 = new Version(7_09_03_99, IndexVersion.V_7_9_3);
    public static final Version V_7_10_0 = new Version(7_10_00_99, IndexVersion.V_7_10_0);
    public static final Version V_7_10_1 = new Version(7_10_01_99, IndexVersion.V_7_10_1);
    public static final Version V_7_10_2 = new Version(7_10_02_99, IndexVersion.V_7_10_2);
    public static final Version V_7_11_0 = new Version(7_11_00_99, IndexVersion.V_7_11_0);
    public static final Version V_7_11_1 = new Version(7_11_01_99, IndexVersion.V_7_11_1);
    public static final Version V_7_11_2 = new Version(7_11_02_99, IndexVersion.V_7_11_2);
    public static final Version V_7_12_0 = new Version(7_12_00_99, IndexVersion.V_7_12_0);
    public static final Version V_7_12_1 = new Version(7_12_01_99, IndexVersion.V_7_12_1);
    public static final Version V_7_13_0 = new Version(7_13_00_99, IndexVersion.V_7_13_0);
    public static final Version V_7_13_1 = new Version(7_13_01_99, IndexVersion.V_7_13_1);
    public static final Version V_7_13_2 = new Version(7_13_02_99, IndexVersion.V_7_13_2);
    public static final Version V_7_13_3 = new Version(7_13_03_99, IndexVersion.V_7_13_3);
    public static final Version V_7_13_4 = new Version(7_13_04_99, IndexVersion.V_7_13_4);
    public static final Version V_7_14_0 = new Version(7_14_00_99, IndexVersion.V_7_14_0);
    public static final Version V_7_14_1 = new Version(7_14_01_99, IndexVersion.V_7_14_1);
    public static final Version V_7_14_2 = new Version(7_14_02_99, IndexVersion.V_7_14_2);
    public static final Version V_7_15_0 = new Version(7_15_00_99, IndexVersion.V_7_15_0);
    public static final Version V_7_15_1 = new Version(7_15_01_99, IndexVersion.V_7_15_1);
    public static final Version V_7_15_2 = new Version(7_15_02_99, IndexVersion.V_7_15_2);
    public static final Version V_7_16_0 = new Version(7_16_00_99, IndexVersion.V_7_16_0);
    public static final Version V_7_16_1 = new Version(7_16_01_99, IndexVersion.V_7_16_1);
    public static final Version V_7_16_2 = new Version(7_16_02_99, IndexVersion.V_7_16_2);
    public static final Version V_7_16_3 = new Version(7_16_03_99, IndexVersion.V_7_16_3);
    public static final Version V_7_17_0 = new Version(7_17_00_99, IndexVersion.V_7_17_0);
    public static final Version V_7_17_1 = new Version(7_17_01_99, IndexVersion.V_7_17_1);
    public static final Version V_7_17_2 = new Version(7_17_02_99, IndexVersion.V_7_17_2);
    public static final Version V_7_17_3 = new Version(7_17_03_99, IndexVersion.V_7_17_3);
    public static final Version V_7_17_4 = new Version(7_17_04_99, IndexVersion.V_7_17_4);
    public static final Version V_7_17_5 = new Version(7_17_05_99, IndexVersion.V_7_17_5);
    public static final Version V_7_17_6 = new Version(7_17_06_99, IndexVersion.V_7_17_6);
    public static final Version V_7_17_7 = new Version(7_17_07_99, IndexVersion.V_7_17_7);
    public static final Version V_7_17_8 = new Version(7_17_08_99, IndexVersion.V_7_17_8);
    public static final Version V_7_17_9 = new Version(7_17_09_99, IndexVersion.V_7_17_9);
    public static final Version V_7_17_10 = new Version(7_17_10_99, IndexVersion.V_7_17_10);
    public static final Version V_7_17_11 = new Version(7_17_11_99, IndexVersion.V_7_17_11);
    public static final Version V_8_0_0 = new Version(8_00_00_99, IndexVersion.V_8_0_0);
    public static final Version V_8_0_1 = new Version(8_00_01_99, IndexVersion.V_8_0_1);
    public static final Version V_8_1_0 = new Version(8_01_00_99, IndexVersion.V_8_1_0);
    public static final Version V_8_1_1 = new Version(8_01_01_99, IndexVersion.V_8_1_1);
    public static final Version V_8_1_2 = new Version(8_01_02_99, IndexVersion.V_8_1_2);
    public static final Version V_8_1_3 = new Version(8_01_03_99, IndexVersion.V_8_1_3);
    public static final Version V_8_2_0 = new Version(8_02_00_99, IndexVersion.V_8_2_0);
    public static final Version V_8_2_1 = new Version(8_02_01_99, IndexVersion.V_8_2_1);
    public static final Version V_8_2_2 = new Version(8_02_02_99, IndexVersion.V_8_2_2);
    public static final Version V_8_2_3 = new Version(8_02_03_99, IndexVersion.V_8_2_3);
    public static final Version V_8_3_0 = new Version(8_03_00_99, IndexVersion.V_8_3_0);
    public static final Version V_8_3_1 = new Version(8_03_01_99, IndexVersion.V_8_3_1);
    public static final Version V_8_3_2 = new Version(8_03_02_99, IndexVersion.V_8_3_2);
    public static final Version V_8_3_3 = new Version(8_03_03_99, IndexVersion.V_8_3_3);
    public static final Version V_8_4_0 = new Version(8_04_00_99, IndexVersion.V_8_4_0);
    public static final Version V_8_4_1 = new Version(8_04_01_99, IndexVersion.V_8_4_1);
    public static final Version V_8_4_2 = new Version(8_04_02_99, IndexVersion.V_8_4_2);
    public static final Version V_8_4_3 = new Version(8_04_03_99, IndexVersion.V_8_4_3);
    public static final Version V_8_5_0 = new Version(8_05_00_99, IndexVersion.V_8_5_0);
    public static final Version V_8_5_1 = new Version(8_05_01_99, IndexVersion.V_8_5_1);
    public static final Version V_8_5_2 = new Version(8_05_02_99, IndexVersion.V_8_5_2);
    public static final Version V_8_5_3 = new Version(8_05_03_99, IndexVersion.V_8_5_3);
    public static final Version V_8_6_0 = new Version(8_06_00_99, IndexVersion.V_8_6_0);
    public static final Version V_8_6_1 = new Version(8_06_01_99, IndexVersion.V_8_6_1);
    public static final Version V_8_6_2 = new Version(8_06_02_99, IndexVersion.V_8_6_2);
    public static final Version V_8_7_0 = new Version(8_07_00_99, IndexVersion.V_8_7_0);
    public static final Version V_8_7_1 = new Version(8_07_01_99, IndexVersion.V_8_7_1);
    public static final Version V_8_8_0 = new Version(8_08_00_99, IndexVersion.V_8_8_0);
    public static final Version V_8_8_1 = new Version(8_08_01_99, IndexVersion.V_8_8_1);
    public static final Version V_8_8_2 = new Version(8_08_02_99, IndexVersion.V_8_8_2);
    public static final Version V_8_8_3 = new Version(8_08_03_99, IndexVersion.V_8_8_3);
    public static final Version V_8_9_0 = new Version(8_09_00_99, IndexVersion.V_8_9_0);
    public static final Version V_8_10_0 = new Version(8_10_00_99, IndexVersion.V_8_10_0);
    public static final Version CURRENT = V_8_10_0;

    private static final NavigableMap<Integer, Version> VERSION_IDS;
    private static final Map<String, Version> VERSION_STRINGS;

    static {
        final NavigableMap<Integer, Version> builder = new TreeMap<>();
        final Map<String, Version> builderByString = new HashMap<>();

        for (final Field declaredField : Version.class.getFields()) {
            if (declaredField.getType().equals(Version.class)) {
                final String fieldName = declaredField.getName();
                if (fieldName.equals("CURRENT") || fieldName.equals("V_EMPTY")) {
                    continue;
                }
                assert fieldName.matches("V_\\d+_\\d+_\\d+") : "expected Version field [" + fieldName + "] to match V_\\d+_\\d+_\\d+";
                try {
                    final Version version = (Version) declaredField.get(null);
                    if (Assertions.ENABLED) {
                        final String[] fields = fieldName.split("_");
                        final int major = Integer.valueOf(fields[1]) * 1000000;
                        final int minor = Integer.valueOf(fields[2]) * 10000;
                        final int revision = Integer.valueOf(fields[3]) * 100;
                        final int expectedId = major + minor + revision + 99;
                        assert version.id == expectedId
                            : "expected version [" + fieldName + "] to have id [" + expectedId + "] but was [" + version.id + "]";
                    }
                    final Version maybePrevious = builder.put(version.id, version);
                    builderByString.put(version.toString(), version);
                    assert maybePrevious == null
                        : "expected [" + version.id + "] to be uniquely mapped but saw [" + maybePrevious + "] and [" + version + "]";
                } catch (final IllegalAccessException e) {
                    assert false : "Version field [" + fieldName + "] should be public";
                }
            }
        }
        assert RestApiVersion.current().major == CURRENT.major && RestApiVersion.previous().major == CURRENT.major - 1
            : "RestApiVersion must be upgraded "
                + "to reflect major from Version.CURRENT ["
                + CURRENT.major
                + "]"
                + " but is still set to ["
                + RestApiVersion.current().major
                + "]";
        builder.put(V_EMPTY_ID, V_EMPTY);
        builderByString.put(V_EMPTY.toString(), V_EMPTY);

        VERSION_IDS = Collections.unmodifiableNavigableMap(builder);
        VERSION_STRINGS = Map.copyOf(builderByString);
    }

    public static Version readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static Version fromId(int id) {
        final Version known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }
        return fromIdSlow(id);
    }

    private static Version fromIdSlow(int id) {
        // TODO: assume this is an old version that has index version == release version
        return new Version(id, IndexVersion.fromId(id));
    }

    public static void writeVersion(Version version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     */
    public static Version min(Version version1, Version version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     */
    public static Version max(Version version1, Version version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static Version fromString(String version) {
        if (Strings.hasLength(version) == false) {
            return Version.CURRENT;
        }
        final Version cached = VERSION_STRINGS.get(version);
        if (cached != null) {
            return cached;
        }
        return fromStringSlow(version);
    }

    private static Version fromStringSlow(String version) {
        final boolean snapshot; // this is some BWC for 2.x and before indices
        if (snapshot = version.endsWith("-SNAPSHOT")) {
            version = version.substring(0, version.length() - 9);
        }
        String[] parts = version.split("[.-]");
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                "the version needs to contain major, minor, and revision, and optionally the build: " + version
            );
        }

        try {
            final int rawMajor = Integer.parseInt(parts[0]);
            if (rawMajor >= 5 && snapshot) { // we don't support snapshot as part of the version here anymore
                throw new IllegalArgumentException("illegal version format - snapshots are only supported until version 2.x");
            }
            if (rawMajor >= 7 && parts.length == 4) { // we don't support qualifier as part of the version anymore
                throw new IllegalArgumentException("illegal version format - qualifiers are only supported until version 6.x");
            }
            if (parts[1].length() > 2) {
                throw new IllegalArgumentException(
                    "illegal minor version format - only one or two digit numbers are supported but found " + parts[1]
                );
            }
            if (parts[2].length() > 2) {
                throw new IllegalArgumentException(
                    "illegal revision version format - only one or two digit numbers are supported but found " + parts[2]
                );
            }
            // we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            final int major = rawMajor * 1000000;
            final int minor = Integer.parseInt(parts[1]) * 10000;
            final int revision = Integer.parseInt(parts[2]) * 100;

            // TODO: 99 is leftover from alpha/beta/rc, it should be removed
            return fromId(major + minor + revision + 99);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    @Deprecated(forRemoval = true)
    public final IndexVersion indexVersion;
    private final String toString;
    private final int previousMajorId;

    Version(int id, IndexVersion indexVersion) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.indexVersion = Objects.requireNonNull(indexVersion);
        this.toString = major + "." + minor + "." + revision;
        this.previousMajorId = major > 0 ? (major - 1) * 1000000 + 99 : major;
    }

    public boolean after(Version version) {
        return version.id < id;
    }

    public boolean onOrAfter(Version version) {
        return version.id <= id;
    }

    public boolean before(Version version) {
        return version.id > id;
    }

    public boolean onOrBefore(Version version) {
        return version.id >= id;
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(this.id, other.id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }

    @Deprecated(forRemoval = true)
    public org.apache.lucene.util.Version luceneVersion() {
        return indexVersion.luceneVersion();
    }

    /*
     * We need the declared versions when computing the minimum compatibility version. As computing the declared versions uses reflection it
     * is not cheap. Since computing the minimum compatibility version can occur often, we use this holder to compute the declared versions
     * lazily once.
     */
    private static class DeclaredVersionsHolder {
        static final List<Version> DECLARED_VERSIONS = List.copyOf(getDeclaredVersions(Version.class));
    }

    // lazy initialized because we don't yet have the declared versions ready when instantiating the cached Version
    // instances
    private Version minCompatVersion;

    // lazy initialized because we don't yet have the declared versions ready when instantiating the cached Version
    // instances
    private Version minIndexCompatVersion;

    /**
     * Returns the minimum compatible version based on the current
     * version. Ie a node needs to have at least the return version in order
     * to communicate with a node running the current version. The returned version
     * is in most of the cases the smallest major version release unless the current version
     * is a beta or RC release then the version itself is returned.
     */
    public Version minimumCompatibilityVersion() {
        Version res = minCompatVersion;
        if (res == null) {
            res = computeMinCompatVersion();
            minCompatVersion = res;
        }
        return res;
    }

    private Version computeMinCompatVersion() {
        if (major == 6) {
            // force the minimum compatibility for version 6 to 5.6 since we don't reference version 5 anymore
            return Version.fromId(5060099);
        } else if (major == 7) {
            // force the minimum compatibility for version 7 to 6.8 since we don't reference version 6 anymore
            return Version.fromId(6080099);
        } else if (major >= 8) {
            // all major versions from 8 onwards are compatible with last minor series of the previous major
            Version bwcVersion = null;

            for (int i = DeclaredVersionsHolder.DECLARED_VERSIONS.size() - 1; i >= 0; i--) {
                final Version candidateVersion = DeclaredVersionsHolder.DECLARED_VERSIONS.get(i);
                if (candidateVersion.major == major - 1 && after(candidateVersion)) {
                    if (bwcVersion != null && candidateVersion.minor < bwcVersion.minor) {
                        break;
                    }
                    bwcVersion = candidateVersion;
                }
            }
            return bwcVersion == null ? this : bwcVersion;
        }

        return Version.min(this, fromId(major * 1000000 + 0 * 10000 + 99));
    }

    /**
     * Returns the minimum created index version that this version supports. Indices created with lower versions
     * can't be used with this version. This should also be used for file based serialization backwards compatibility ie. on serialization
     * code that is used to read / write file formats like transaction logs, cluster state, and index metadata.
     */
    public Version minimumIndexCompatibilityVersion() {
        Version res = minIndexCompatVersion;
        if (res == null) {
            res = computeMinIndexCompatVersion();
            minIndexCompatVersion = res;
        }
        return res;
    }

    private Version computeMinIndexCompatVersion() {
        final int bwcMajor;
        if (major == 5) {
            bwcMajor = 2; // we jumped from 2 to 5
        } else {
            bwcMajor = major - 1;
        }
        final int bwcMinor = 0;
        return Version.min(this, fromId(bwcMajor * 1000000 + bwcMinor * 10000 + 99));
    }

    /**
     * Whether the current version is older than the current minimum compatible index version,
     * see {@link #minimumIndexCompatibilityVersion()}
     */
    public boolean isLegacyIndexVersion() {
        return before(Version.CURRENT.minimumIndexCompatibilityVersion());
    }

    /**
     * Returns <code>true</code> iff both version are compatible. Otherwise <code>false</code>
     */
    public boolean isCompatible(Version version) {
        boolean compatible = onOrAfter(version.minimumCompatibilityVersion()) && version.onOrAfter(minimumCompatibilityVersion());

        assert compatible == false || Math.max(major, version.major) - Math.min(major, version.major) <= 1;
        return compatible;
    }

    /**
     * Returns a first major version previous to the version stored in this object.
     * I.e 8.1.0 will return 7.0.0
     */
    public Version previousMajor() {
        return Version.fromId(previousMajorId);
    }

    @SuppressForbidden(reason = "System.out.*")
    public static void main(String[] args) {
        final String versionOutput = String.format(
            Locale.ROOT,
            "Version: %s, Build: %s/%s/%s, JVM: %s",
            Build.CURRENT.qualifiedVersion(),
            Build.CURRENT.type().displayName(),
            Build.CURRENT.hash(),
            Build.CURRENT.date(),
            JvmInfo.jvmInfo().version()
        );
        System.out.println(versionOutput);
    }

    @Override
    public String toString() {
        return toString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version = (Version) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }

    /**
     * Extracts a sorted list of declared version constants from a class.
     * The argument would normally be Version.class but is exposed for
     * testing with other classes-containing-version-constants.
     */
    public static List<Version> getDeclaredVersions(final Class<?> versionClass) {
        final Field[] fields = versionClass.getFields();
        final List<Version> versions = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final int mod = field.getModifiers();
            if (false == Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                continue;
            }
            if (field.getType() != Version.class) {
                continue;
            }
            switch (field.getName()) {
                case "CURRENT":
                case "V_EMPTY":
                    continue;
            }
            assert field.getName().matches("V(_\\d+){3}?") : field.getName();
            try {
                versions.add(((Version) field.get(null)));
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(versions);
        return versions;
    }
}
