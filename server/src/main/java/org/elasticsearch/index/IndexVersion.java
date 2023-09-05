/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.internal.VersionExtension;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * The index version.
 * <p>
 * Prior to 8.8.0, the node {@link Version} was used everywhere. This class separates the index format version
 * from the running node version.
 * <p>
 * Each index version constant has an id number, which for versions prior to 8.9.0 is the same as the release version
 * for backwards compatibility. In 8.9.0 this is changed to an incrementing number, disconnected from the release version.
 * <p>
 * Each version constant has a unique id string. This is not actually stored in the index, but is there to ensure
 * each index version is only added to the source file once. This string needs to be unique (normally a UUID,
 * but can be any other unique nonempty string).
 * If two concurrent PRs add the same index version, the different unique ids cause a git conflict, ensuring the second PR to be merged
 * must be updated with the next free version first. Without the unique id string, git will happily merge the two versions together,
 * resulting in the same index version being used across multiple commits,
 * causing problems when you try to upgrade between those two merged commits.
 * <h2>Version compatibility</h2>
 * The earliest compatible version is hardcoded in the {@link #MINIMUM_COMPATIBLE} field. Previously, this was dynamically calculated
 * from the major/minor versions of {@link Version}, but {@code IndexVersion} does not have separate major/minor version numbers.
 * So the minimum compatible version is hard-coded as the index version used by the first version of the previous major release.
 * {@link #MINIMUM_COMPATIBLE} should be updated appropriately whenever a major release happens.
 * <h2>Adding a new version</h2>
 * A new index version should be added <em>every time</em> a change is made to the serialization protocol of one or more classes.
 * Each index version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new index version, add a new constant at the bottom of the list that is one greater than the current highest version,
 * ensure it has a unique id, and update the {@link #current()} constant to point to the new version.
 * <h2>Reverting an index version</h2>
 * If you revert a commit with an index version change, you <em>must</em> ensure there is a <em>new</em> index version
 * representing the reverted change. <em>Do not</em> let the index version go backwards, it must <em>always</em> be incremented.
 */
@SuppressWarnings({"checkstyle:linelength", "deprecation"})
public record IndexVersion(int id, Version luceneVersion) implements VersionId<IndexVersion>, ToXContentFragment {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerIndexVersion method.
     * When all the index version constants have been registered, the map is cleared & never touched again.
     */
    @SuppressWarnings("UnusedAssignment")
    private static Map<String, Integer> IDS = new HashMap<>();

    private static IndexVersion registerIndexVersion(int id, Version luceneVersion, String uniqueId) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        Strings.requireNonEmpty(uniqueId, "Each IndexVersion needs a unique string id");
        Integer existing = IDS.put(uniqueId, id);
        if (existing != null) {
            throw new IllegalArgumentException("Versions " + id + " and " + existing + " have the same unique id");
        }
        return new IndexVersion(id, luceneVersion);
    }

    public static final IndexVersion ZERO = registerIndexVersion(0, Version.LATEST, "00000000-0000-0000-0000-000000000000");
    public static final IndexVersion V_7_0_0 = registerIndexVersion(7_00_00_99, Version.LUCENE_8_0_0, "b32be92d-c403-4858-a4a3-20d699a47ae6");
    public static final IndexVersion V_7_1_0 = registerIndexVersion(7_01_00_99, Version.LUCENE_8_0_0, "f9964d87-9f20-4b26-af32-be1f979216ec");
    public static final IndexVersion V_7_2_0 = registerIndexVersion(7_02_00_99, Version.LUCENE_8_0_0, "dba49448-87d4-45bb-ba19-f7b4eb85c757");
    public static final IndexVersion V_7_2_1 = registerIndexVersion(7_02_01_99, Version.LUCENE_8_0_0, "58874b45-f9f8-4c04-92a9-67548a8b21c3");
    public static final IndexVersion V_7_3_0 = registerIndexVersion(7_03_00_99, Version.LUCENE_8_1_0, "3d8a21df-58a4-4d7a-ba5d-438c92c16a7b");
    public static final IndexVersion V_7_4_0 = registerIndexVersion(7_04_00_99, Version.LUCENE_8_2_0, "c1fe73ba-0173-476c-aba2-855c2b31ac18");
    public static final IndexVersion V_7_5_0 = registerIndexVersion(7_05_00_99, Version.LUCENE_8_3_0, "ab08ae25-ede2-4e57-a43f-89d96aa989e4");
    public static final IndexVersion V_7_5_2 = registerIndexVersion(7_05_02_99, Version.LUCENE_8_3_0, "706715ca-3b91-40d2-8c2e-c34c459b5d0d");
    public static final IndexVersion V_7_6_0 = registerIndexVersion(7_06_00_99, Version.LUCENE_8_4_0, "63acbdb9-51c8-4976-bb3d-e55052a4fbd4");
    public static final IndexVersion V_7_7_0 = registerIndexVersion(7_07_00_99, Version.LUCENE_8_5_1, "6fff8238-e6ce-4fb2-85de-196492026e49");
    public static final IndexVersion V_7_8_0 = registerIndexVersion(7_08_00_99, Version.LUCENE_8_5_1, "81d7d459-f386-4c20-8235-f8fce8af7f0e");
    public static final IndexVersion V_7_9_0 = registerIndexVersion(7_09_00_99, Version.LUCENE_8_6_0, "0fa951a2-43ce-4f76-91bf-066c1ecf8a93");
    public static final IndexVersion V_7_10_0 = registerIndexVersion(7_10_00_99, Version.LUCENE_8_7_0, "92ccd91c-0251-4263-8873-9f1abfac3c10");
    public static final IndexVersion V_7_11_0 = registerIndexVersion(7_11_00_99, Version.LUCENE_8_7_0, "e6d65f96-26d5-4669-ac5a-2964b9b1699f");
    public static final IndexVersion V_7_12_0 = registerIndexVersion(7_12_00_99, Version.LUCENE_8_8_0, "39e2989a-a9a4-4f1a-b185-2e6015f74b1c");
    public static final IndexVersion V_7_13_0 = registerIndexVersion(7_13_00_99, Version.LUCENE_8_8_2, "28b21fe0-4d1f-4c04-95cc-74df494ae0cf");
    public static final IndexVersion V_7_14_0 = registerIndexVersion(7_14_00_99, Version.LUCENE_8_9_0, "b45bb223-bb73-4379-a46f-7dc74d38aaca");
    public static final IndexVersion V_7_15_0 = registerIndexVersion(7_15_00_99, Version.LUCENE_8_9_0, "ab666b02-b866-4b64-9ba3-d511e86c55b5");
    public static final IndexVersion V_7_16_0 = registerIndexVersion(7_16_00_99, Version.LUCENE_8_10_1, "a582e900-2d92-474c-9be3-2e08fa88be4b");
    public static final IndexVersion V_7_17_0 = registerIndexVersion(7_17_00_99, Version.LUCENE_8_11_1, "18766ab8-4691-40a2-94f1-526f3b71420c");
    public static final IndexVersion V_8_0_0 = registerIndexVersion(8_00_00_99, Version.LUCENE_9_0_0, "ff18a13c-1fa7-4cf7-a3b1-4fdcd9461d5b");
    public static final IndexVersion V_8_1_0 = registerIndexVersion(8_01_00_99, Version.LUCENE_9_0_0, "b4742461-ee43-4fd0-a260-29f8388b82ec");
    public static final IndexVersion V_8_2_0 = registerIndexVersion(8_02_00_99, Version.LUCENE_9_1_0, "af0ed990-2f32-42b5-aaf3-59d21a3dca7a");
    public static final IndexVersion V_8_3_0 = registerIndexVersion(8_03_00_99, Version.LUCENE_9_2_0, "eca8e8a3-0724-4247-a58d-e4eafcec4b3f");
    public static final IndexVersion V_8_4_0 = registerIndexVersion(8_04_00_99, Version.LUCENE_9_3_0, "d27324da-b36c-452a-93a8-9b69a6c302a1");
    public static final IndexVersion V_8_5_0 = registerIndexVersion(8_05_00_99, Version.LUCENE_9_4_1, "c5284b51-7fee-4f34-a837-241bb57a7aa6");
    public static final IndexVersion V_8_6_0 = registerIndexVersion(8_06_00_99, Version.LUCENE_9_4_2, "5e78c76c-74aa-464e-9383-89bdffb74db9");
    public static final IndexVersion V_8_7_0 = registerIndexVersion(8_07_00_99, Version.LUCENE_9_5_0, "f9227941-d6f4-462b-957f-2bcd36c28382");
    public static final IndexVersion V_8_8_0 = registerIndexVersion(8_08_00_99, Version.LUCENE_9_6_0, "d6ffc8d7-f6bd-469b-8495-01688c310000");
    public static final IndexVersion V_8_8_2 = registerIndexVersion(8_08_02_99, Version.LUCENE_9_6_0, "9db9d888-6be8-4a58-825c-f423fd8c6b00");
    public static final IndexVersion V_8_9_0 = registerIndexVersion(8_09_00_99, Version.LUCENE_9_7_0, "32f6dbab-cc24-4f5b-87b5-015a848480d9");
    public static final IndexVersion V_8_9_1 = registerIndexVersion(8_09_01_99, Version.LUCENE_9_7_0, "955a80ac-f70c-40a5-9399-1d8a1e5d342d");
    public static final IndexVersion V_8_10_0 = registerIndexVersion(8_10_00_99, Version.LUCENE_9_7_0, "2e107286-12ad-4c51-9a6f-f8943663b6e7");
    public static final IndexVersion V_8_11_0 = registerIndexVersion(8_11_00_99, Version.LUCENE_9_7_0, "f08382c0-06ab-41f4-a56a-cf5397275627");

    /*
     * READ THE COMMENT BELOW THIS BLOCK OF DECLARATIONS BEFORE ADDING NEW INDEX VERSIONS
     * Detached index versions added below here.
     */
    public static final IndexVersion V_8_500_000 = registerIndexVersion(8_500_000, Version.LUCENE_9_7_0, "bf656f5e-5808-4eee-bf8a-e2bf6736ff55");
    public static final IndexVersion V_8_500_001 = registerIndexVersion(8_500_001, Version.LUCENE_9_7_0, "45045a5a-fc57-4462-89f6-6bc04cda6015");
    /*
     * STOP! READ THIS FIRST! No, really,
     *        ____ _____ ___  ____  _        ____  _____    _    ____    _____ _   _ ___ ____    _____ ___ ____  ____ _____ _
     *       / ___|_   _/ _ \|  _ \| |      |  _ \| ____|  / \  |  _ \  |_   _| | | |_ _/ ___|  |  ___|_ _|  _ \/ ___|_   _| |
     *       \___ \ | || | | | |_) | |      | |_) |  _|   / _ \ | | | |   | | | |_| || |\___ \  | |_   | || |_) \___ \ | | | |
     *        ___) || || |_| |  __/|_|      |  _ <| |___ / ___ \| |_| |   | | |  _  || | ___) | |  _|  | ||  _ < ___) || | |_|
     *       |____/ |_| \___/|_|   (_)      |_| \_\_____/_/   \_\____/    |_| |_| |_|___|____/  |_|   |___|_| \_\____/ |_| (_)
     *
     * A new index version should be added EVERY TIME a change is made to index metadata or data storage.
     * Each index version should only be used in a single merged commit (apart from the BwC versions copied from o.e.Version, ≤V_8_11_0).
     *
     * To add a new index version, add a new constant at the bottom of the list, above this comment, which is one greater than the
     * current highest version, ensure it has a fresh UUID, and update CurrentHolder#CURRENT to point to the new version. Don't add other
     * lines, comments, etc.
     *
     * REVERTING AN INDEX VERSION
     *
     * If you revert a commit with an index version change, you MUST ensure there is a NEW index version representing the reverted
     * change. DO NOT let the index version go backwards, it must ALWAYS be incremented.
     *
     * DETERMINING TRANSPORT VERSIONS FROM GIT HISTORY
     *
     * TODO after the release of v8.11.0, copy the instructions about using git to track the history of versions from TransportVersion.java
     * (the example commands won't make sense until at least 8.11.0 is released)
     */

    private static class CurrentHolder {
        private static final IndexVersion CURRENT = findCurrent(V_8_500_001);

        // finds the pluggable current version, or uses the given fallback
        private static IndexVersion findCurrent(IndexVersion fallback) {
            var versionExtension = VersionExtension.load();
            if (versionExtension == null) {
                return fallback;
            }
            var version = versionExtension.getCurrentIndexVersion();

            assert version.onOrAfter(fallback);
            assert version.luceneVersion.equals(Version.LATEST)
                : "IndexVersion must be upgraded to ["
                + Version.LATEST
                + "] is still set to ["
                + version.luceneVersion
                + "]";
            return version;
        }
    }

    public static final IndexVersion MINIMUM_COMPATIBLE = V_7_0_0;

    static {
        // see comment on IDS field
        // now we're registered the index versions, we can clear the map
        IDS = null;
    }

    static NavigableMap<Integer, IndexVersion> getAllVersionIds(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        NavigableMap<Integer, IndexVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");

        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(IndexVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }

                IndexVersion version;
                try {
                    version = (IndexVersion) declaredField.get(null);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
                builder.put(version.id, version);

                if (Assertions.ENABLED) {
                    // check the version number is unique
                    var sameVersionNumber = versionIdFields.put(version.id, fieldName);
                    assert sameVersionNumber == null
                        : "Versions ["
                        + sameVersionNumber
                        + "] and ["
                        + fieldName
                        + "] have the same version number ["
                        + version.id
                        + "]. Each IndexVersion should have a different version number";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, IndexVersion> VERSION_IDS;

    static {
        VERSION_IDS = getAllVersionIds(IndexVersion.class);
    }

    static Collection<IndexVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    public static IndexVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static IndexVersion fromId(int id) {
        IndexVersion known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }

        // this is a version we don't otherwise know about
        // We need to guess the lucene version.
        // Our best guess is to use the same lucene version as the previous
        // version in the list, assuming that it didn't change.
        // if it's older than any known version use the previous major to the oldest known lucene version
        var prev = VERSION_IDS.floorEntry(id);
        Version luceneVersion = prev != null
            ? prev.getValue().luceneVersion
            : Version.fromBits(VERSION_IDS.firstEntry().getValue().luceneVersion.major - 1, 0, 0);

        return new IndexVersion(id, luceneVersion);
    }

    public static void writeVersion(IndexVersion version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     */
    public static IndexVersion min(IndexVersion version1, IndexVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     */
    public static IndexVersion max(IndexVersion version1, IndexVersion version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    /**
     * Returns the most recent index version.
     * This should be the index version with the highest id.
     */
    public static IndexVersion current() {
        return CurrentHolder.CURRENT;
    }

    public boolean isLegacyIndexVersion() {
        return before(MINIMUM_COMPATIBLE);
    }

    public static IndexVersion getMinimumCompatibleIndexVersion(int versionId) {
        int major = versionId / 1_000_000;
        if (major == IndexVersion.current().id() / 1_000_000) {
            // same compatibility version as current
            return IndexVersion.MINIMUM_COMPATIBLE;
        } else {
            int compatId = (major-1) * 1_000_000;
            if (major <= 8) compatId += 99;
            return IndexVersion.fromId(compatId);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexVersion version = (IndexVersion) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
