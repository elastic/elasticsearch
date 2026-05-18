/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The version number associated with various ML features. This class is needed in addition to TransportVersion because
 * transport version cannot be persisted in stored documents or cluster state metadata.
 * Hence, this class is designed to be persisted in human-readable format, and indicate the age of that state or config.
 * In addition, we want the written form of MlConfigVersion version numbers to be parseable by the {@link Version} class so that
 * in mixed version clusters during upgrades the old nodes won't throw exceptions when parsing these new versions.
 * <p>
 * Prior to 8.10.0, the release {@link Version} was used everywhere. This class separates the ML config format version
 * from the running node version.
 * <p>
 * Each ML config version constant has an id number, which for versions prior to 8.10.0 is the same as the release version
 * for backwards compatibility. In 8.10.0 this is changed to an incrementing number, disconnected from the release version,
 * starting at 10000099. This format is chosen for best compatibility with old node versions.
 * * <p>
 * Each version constant has a unique id string. This is not actually used in the binary protocol, but is there to ensure
 * each protocol version is only added to the source file once. This string needs to be unique (normally a UUID,
 * but can be any other unique nonempty string).
 * If two concurrent PRs add the same ML config version, the different unique ids cause a git conflict, ensuring the second PR to be merged
 * must be updated with the next free version first. Without the unique id string, git will happily merge the two versions together,
 * resulting in the same ML config version being used across multiple commits, causing problems when you try to upgrade between those two
 * merged commits.
 * <h2>Version compatibility</h2>
 * The earliest version is hardcoded in the {@link #FIRST_ML_VERSION} field. This cannot be dynamically calculated
 * from the major/minor versions of {@link Version}, because {@code MlConfigVersion} does not have separate major/minor version numbers.
 * So the minimum version is simply hard-coded as the earliest version where ML existed (5.4.0).
 * <h2>Adding a new version</h2>
 * A new ML config version should be added <em>every time</em> a change is made to the serialization format of one or more ML config
 * or state classes.
 * Each ML config version should only be used in a single merged commit (apart from BwC versions copied from {@link Version}).
 * <p>
 * To add a new ML config version, add a new constant at the bottom of the list that is one million greater than the current highest
 * version, ensure it has a unique id, and update the {@link #CURRENT} constant to point to the new version.
 * <h2>Reverting a ML config version</h2>
 * If you revert a commit with a ML config version change, you <em>must</em> ensure there is a <em>new</em> ML config version
 * representing the reverted change. <em>Do not</em> let the ML config version go backwards, it must <em>always</em> be incremented.
 */
public record MlConfigVersion(int id) implements VersionId<MlConfigVersion>, ToXContentFragment {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerMlConfigVersion method.
     * When all the ML config version constants have been registered, the map is cleared & never touched again.
     */
    private static Map<String, Integer> IDS = new HashMap<>();

    private static MlConfigVersion registerMlConfigVersion(int id, String uniqueId) {
        checkUniqueness(id, uniqueId);
        return new MlConfigVersion(id);
    }

    private static void checkUniqueness(int id, String uniqueId) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        Strings.requireNonEmpty(uniqueId, "Each MlConfigVersion needs a unique string id");
        Integer existing = IDS.put(uniqueId, id);
        if (existing != null) {
            throw new IllegalArgumentException("Versions " + id + " and " + existing + " have the same unique id");
        }
    }

    public static final String ML_CONFIG_VERSION_NODE_ATTR = "ml.config_version";

    public static final MlConfigVersion ZERO = registerMlConfigVersion(0, "00000000-0000-0000-0000-000000000000");
    public static final MlConfigVersion V_5_4_0 = registerMlConfigVersion(5_04_00_99, "B917B447-2CC6-4BD8-BEB2-733D157B7B37");
    public static final MlConfigVersion V_6_0_0 = registerMlConfigVersion(6_00_00_99, "E3F1CC55-A992-4CB3-BCE6-EC310D156793");
    public static final MlConfigVersion V_7_0_0 = registerMlConfigVersion(7_00_00_99, "21906C88-0955-4BE3-B4B3-BFB6B9606112");
    public static final MlConfigVersion V_7_0_1 = registerMlConfigVersion(7_00_01_99, "7FF09B06-20B3-40FF-A6E0-EC94F55B8B56");
    public static final MlConfigVersion V_7_1_0 = registerMlConfigVersion(7_01_00_99, "BD49F37F-1E50-4BC1-B227-EE0AC88B677C");
    public static final MlConfigVersion V_7_2_0 = registerMlConfigVersion(7_02_00_99, "4DCD30C3-FF6B-4195-AD28-4600D48F4A7B");
    public static final MlConfigVersion V_7_2_1 = registerMlConfigVersion(7_02_01_99, "0C6EDC80-77C9-4897-843B-EC4377E997FD");
    public static final MlConfigVersion V_7_3_0 = registerMlConfigVersion(7_03_00_99, "69107EE3-48A7-4FC2-BDB3-7440C4B3E0D4");
    public static final MlConfigVersion V_7_3_2 = registerMlConfigVersion(7_03_02_99, "2155846F-36F5-420B-BD29-6415019AB64A");
    public static final MlConfigVersion V_7_4_0 = registerMlConfigVersion(7_04_00_99, "21E66DF1-9AAE-40E8-9EF0-E4494CF513B6");
    public static final MlConfigVersion V_7_5_0 = registerMlConfigVersion(7_05_00_99, "ED5D4FA8-E22B-4ABD-A69E-76BBE9363CDD");
    public static final MlConfigVersion V_7_6_0 = registerMlConfigVersion(7_06_00_99, "1155C404-6ABA-4782-9A6D-FD7167D4109C");
    public static final MlConfigVersion V_7_7_0 = registerMlConfigVersion(7_07_00_99, "5369472F-3845-4759-9927-875DCACB66FC");
    public static final MlConfigVersion V_7_8_0 = registerMlConfigVersion(7_08_00_99, "94F8181D-4F74-4C22-9015-7BE6D2ACA9FE");
    public static final MlConfigVersion V_7_8_1 = registerMlConfigVersion(7_08_01_99, "1522F9FD-CAD5-4221-95A8-61C22C9AA4FD");
    public static final MlConfigVersion V_7_9_0 = registerMlConfigVersion(7_09_00_99, "42557660-0127-47B3-B01F-B02E2A5B47DB");
    public static final MlConfigVersion V_7_9_1 = registerMlConfigVersion(7_09_01_99, "0AA38DE2-74C0-4D36-BCA9-AAB2EE728D2C");
    public static final MlConfigVersion V_7_9_2 = registerMlConfigVersion(7_09_02_99, "165B1C7D-51A7-4FC6-A673-28885C2403E0");
    public static final MlConfigVersion V_7_9_3 = registerMlConfigVersion(7_09_03_99, "4B9B8DB4-D1C1-4F8B-994A-3352CE4C376E");
    public static final MlConfigVersion V_7_10_0 = registerMlConfigVersion(7_10_00_99, "389FEA3F-1A53-4F26-AEBA-584EF954596F");
    public static final MlConfigVersion V_7_10_1 = registerMlConfigVersion(7_10_01_99, "ACA28B55-87B8-4F87-9517-7B3345F1F789");
    public static final MlConfigVersion V_7_11_0 = registerMlConfigVersion(7_11_00_99, "0377318D-6D53-41C5-A376-8E42E1435806");
    public static final MlConfigVersion V_7_12_0 = registerMlConfigVersion(7_12_00_99, "0CB23517-70F2-4F26-9E7D-8F6FCD16B14A");
    public static final MlConfigVersion V_7_13_0 = registerMlConfigVersion(7_13_00_99, "49BE3B14-9E33-48A2-B605-DB0B61AAD49D");
    public static final MlConfigVersion V_7_14_0 = registerMlConfigVersion(7_14_00_99, "DD87203A-2CBD-40A6-A466-70D9F560B282");
    public static final MlConfigVersion V_7_15_0 = registerMlConfigVersion(7_15_00_99, "890AA965-41D4-4C99-9493-2CA8BBD06C99");
    public static final MlConfigVersion V_7_15_1 = registerMlConfigVersion(7_15_01_99, "0C0EC66A-DAD7-4448-99F6-A13D465D5203");
    public static final MlConfigVersion V_7_16_0 = registerMlConfigVersion(7_16_00_99, "F4C8C99C-EC0D-4119-AD6A-CFEEAFE6461C");
    public static final MlConfigVersion V_7_17_0 = registerMlConfigVersion(7_17_00_99, "056316CB-61CB-4A5D-84BB-8ACDA24D5748");
    public static final MlConfigVersion V_7_17_1 = registerMlConfigVersion(7_17_01_99, "A0006AC5-7C5A-42CC-97F7-8792FC8DEEAC");
    public static final MlConfigVersion V_7_17_8 = registerMlConfigVersion(7_17_08_99, "A769CAD5-0A3F-4CB4-BC0C-9CFDCC6D6605");
    public static final MlConfigVersion V_8_0_0 = registerMlConfigVersion(8_00_00_99, "2B5AFCFD-90F9-41CD-9E55-4D2711F03758");
    public static final MlConfigVersion V_8_1_0 = registerMlConfigVersion(8_01_00_99, "4E1B967E-4BFA-4C41-8F59-E43C87B1DEFF");
    public static final MlConfigVersion V_8_2_0 = registerMlConfigVersion(8_02_00_99, "01E035F3-D864-4094-8523-84D717EFB89D");
    public static final MlConfigVersion V_8_3_0 = registerMlConfigVersion(8_03_00_99, "574B950D-2E9F-4CCA-9E61-F93B4420D800");
    public static final MlConfigVersion V_8_4_0 = registerMlConfigVersion(8_04_00_99, "D92DAC63-DAE6-4858-9E63-9BD91B67459A");
    public static final MlConfigVersion V_8_5_0 = registerMlConfigVersion(8_05_00_99, "178A2E20-8F8E-4430-A002-B156E1E99B22");
    public static final MlConfigVersion V_8_6_0 = registerMlConfigVersion(8_06_00_99, "C910CB2A-A7B5-47AE-A9F5-FAC8F5205D27");
    public static final MlConfigVersion V_8_6_1 = registerMlConfigVersion(8_06_01_99, "DFD4116C-1173-4461-8A00-00E0ADCAD2C2");
    public static final MlConfigVersion V_8_7_0 = registerMlConfigVersion(8_07_00_99, "C9A8AACB-84FA-44C6-A541-2FDEAECB280D");
    public static final MlConfigVersion V_8_7_1 = registerMlConfigVersion(8_07_01_99, "B2EC2F2F-9D73-4057-A21C-23E0EF3AD311");
    public static final MlConfigVersion V_8_8_0 = registerMlConfigVersion(8_08_00_99, "8E50EED5-54E3-45B1-A3B2-83A44ADBBF09");
    public static final MlConfigVersion V_8_8_1 = registerMlConfigVersion(8_08_01_99, "99A928F3-FD13-4325-9770-317EB624C85C");
    public static final MlConfigVersion V_8_9_0 = registerMlConfigVersion(8_09_00_99, "C50F56AB-4DB8-48A5-9467-4F5B07365C5C");

    // This constant should never be tested externally - it's considered the same as V_10 externally
    private static final MlConfigVersion V_8_10_0 = registerMlConfigVersion(8_10_00_99, "9315A548-D81B-4FE7-8C0D-0DA81EA00F9E");

    /*
     * READ THE JAVADOC ABOVE BEFORE ADDING NEW ML CONFIG VERSIONS
     * Detached ML config versions added below here.
     */

    public static final MlConfigVersion V_10 = registerMlConfigVersion(10_00_00_99, "4B940FD9-BEDD-4589-8E08-02D9B480B22D");
    // V_11 is used in ELSER v2 package configs
    public static final MlConfigVersion V_11 = registerMlConfigVersion(11_00_0_0_99, "79CB2950-57C7-11EE-AE5D-0800200C9A66");
    public static final MlConfigVersion V_12 = registerMlConfigVersion(12_00_0_0_99, "Trained model config prefix strings added");

    /**
     * Reference to the most recent Ml config version.
     * This should be the Ml config version with the highest id.
     */
    public static final MlConfigVersion CURRENT = V_12;

    /**
     * Reference to the first MlConfigVersion that is detached from the
     * stack (product) version.
     */
    public static final MlConfigVersion MINIMUM_DETACHED_ML_CONFIG_VERSION = V_10;

    /**
     * Reference to the earliest compatible ML config version to this version of the codebase.
     * This is hard-coded as the first version that included the ML plugin.
     */
    public static final MlConfigVersion FIRST_ML_VERSION = V_5_4_0;

    static {
        // see comment on IDS field
        // now we've registered all the ML config versions, we can clear the map
        IDS = null;
    }

    /**
     * Obtain a selection of (nearly) all registered versions.
     * This method should only ever be used internally - to initialize VERSION_IDS,
     * and in unit tests. It should never be called directly in production code.
     */
    public static NavigableMap<Integer, MlConfigVersion> getAllVersionIds(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        NavigableMap<Integer, MlConfigVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("V_8_10_0", "ZERO", "CURRENT", "FIRST_ML_VERSION");

        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(MlConfigVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }
                if (fieldName.matches("V_.*") == false) {
                    continue;
                }

                MlConfigVersion version;
                try {
                    version = (MlConfigVersion) declaredField.get(null);
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
                            + "]. Each MlConfigVersion should have a different version number";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, MlConfigVersion> VERSION_IDS = getAllVersionIds(MlConfigVersion.class);

    static Collection<MlConfigVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    public static MlConfigVersion readVersion(StreamInput in) throws IOException {
        // For version 8.10.0 and earlier this must be binary compatible with Version.writeVersion()
        return fromId(in.readVInt());
    }

    public static MlConfigVersion fromId(int id) {
        MlConfigVersion known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }
        // this is a version we don't otherwise know about - just create a placeholder
        return new MlConfigVersion(id);
    }

    public static MlConfigVersion fromNode(DiscoveryNode node) {
        return getMlConfigVersionForNode(node);
    }

    public static void writeVersion(MlConfigVersion version, StreamOutput out) throws IOException {
        // For version 8.10.0 and earlier this must be binary compatible with Version.readVersion()
        out.writeVInt(version.id);
    }

    /**
     * Returns the minimum version of {@code version1} and {@code version2}
     * Legacy, semantic style representations are treated as being before all new, single number representations
     */
    public static MlConfigVersion min(MlConfigVersion version1, MlConfigVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the maximum version of {@code version1} and {@code version2}
     * New, single number representations are treated as being after all legacy, semantic style representations.
     */
    public static MlConfigVersion max(MlConfigVersion version1, MlConfigVersion version2) {
        return version1.id > version2.id ? version1 : version2;
    }

    public static MlConfigVersion getMinMlConfigVersion(DiscoveryNodes nodes) {
        return getMinMaxMlConfigVersion(nodes).v1();
    }

    public static MlConfigVersion getMaxMlConfigVersion(DiscoveryNodes nodes) {
        return getMinMaxMlConfigVersion(nodes).v2();
    }

    public static Tuple<MlConfigVersion, MlConfigVersion> getMinMaxMlConfigVersion(DiscoveryNodes nodes) {
        MlConfigVersion minMlConfigVersion = MlConfigVersion.CURRENT;
        MlConfigVersion maxMlConfigVersion = MlConfigVersion.FIRST_ML_VERSION;

        // many nodes will have the same versions, so de-duplicate versions first so that we only have to parse unique versions
        final Set<String> versions = new HashSet<>();
        for (DiscoveryNode node : nodes) {
            String mlConfigVerStr = node.getAttributes().get(ML_CONFIG_VERSION_NODE_ATTR);
            if (mlConfigVerStr != null) { // ignore nodes that don't have an ML config version
                versions.add(mlConfigVerStr);
            }
        }

        // of the unique versions, find the min and max
        for (String version : versions) {
            MlConfigVersion mlConfigVersion = fromString(version);
            if (mlConfigVersion.before(minMlConfigVersion)) {
                minMlConfigVersion = mlConfigVersion;
            }
            if (mlConfigVersion.after(maxMlConfigVersion)) {
                maxMlConfigVersion = mlConfigVersion;
            }
        }
        return new Tuple<>(minMlConfigVersion, maxMlConfigVersion);
    }

    public static MlConfigVersion getMlConfigVersionForNode(DiscoveryNode node) {
        String mlConfigVerStr = node.getAttributes().get(ML_CONFIG_VERSION_NODE_ATTR);
        if (mlConfigVerStr == null) throw new IllegalStateException(ML_CONFIG_VERSION_NODE_ATTR + " not present on node");
        return fromString(mlConfigVerStr);
    }

    // supports fromString below
    private static final Pattern ML_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)(?:-\\w+)?$");

    // Parse an MlConfigVersion from a string.
    // Note that version "8.10.x" and "8.11.0" are silently converted to "10.0.0".
    // This is to support upgrade scenarios in pre-prod QA environments.
    public static MlConfigVersion fromString(String str) {
        if (str == null) {
            return CURRENT;
        }
        // The whole switch from Version to MlConfigVersion was supposed to take
        // place during development of 8.10.0, however, one place was missed. As
        // a result there may be DFA destination indices in the wild with metadata
        // containing 8.10.1, 8.10.2, 8.10.3 or 8.11.0. We can treat these as V_10
        // for config version comparison purposes.
        if (str.startsWith("8.10.") || str.equals("8.11.0")) {
            return V_10;
        }
        Matcher matcher = ML_VERSION_PATTERN.matcher(str);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("ML config version [" + str + "] not valid");
        }
        int first = Integer.parseInt(matcher.group(1));
        int second = Integer.parseInt(matcher.group(2));
        int third = Integer.parseInt(matcher.group(3));
        if (first >= 10 && (second > 0 || third > 0)) {
            throw new IllegalArgumentException("ML config version [" + str + "] not valid");
        }
        return fromId(1000000 * first + 10000 * second + 100 * third + 99);
    }

    @Override
    public String toString() {
        final int major = id / 1000000;
        if (id < MINIMUM_DETACHED_ML_CONFIG_VERSION.id()) {
            final int minor = (id % 1000000) / 10000;
            final int patch = ((id % 1000000) % 10000) / 100;
            return major + "." + minor + "." + patch;
        }
        return major + ".0.0";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder.value(toString());
    }
}
