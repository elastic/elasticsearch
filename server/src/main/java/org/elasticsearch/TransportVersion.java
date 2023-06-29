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
 * Represents the version of the wire protocol used to communicate between ES nodes.
 * <p>
 * Prior to 8.7.0, the node {@link Version} was used everywhere. This class separates the wire protocol version
 * from the running node version. Each node version has a reference to a specific transport version used by that node.
 * <p>
 * Each transport version constant has an id number, which for versions prior to 8.7.0 is the same as the node version
 * for backwards compatibility.
 * There is also a unique id string. This is not actually used in the protocol, but is there to ensure each protocol version
 * is only added to the source file once. This string needs to be unique (here, a UUID, but can be any other unique nonempty string).
 * If two concurrent PRs added the same protocol version, the unique string causes a git conflict, ensuring the second PR to be merged
 * must be updated with the next free version. Without the unique id string, git will happily merge the two versions together,
 * causing problems when you try to upgrade between those two PRs.
 * <p>
 * When adding new transport versions, it is recommended to leave a gap in the id number (say, 100)
 * to leave space for any intermediate fixes that may be needed in the future.
 * <p>
 * The earliest compatible version is hardcoded at {@link #MINIMUM_COMPATIBLE}. Previously, this was dynamically calculated
 * from the major/minor versions of {@link Version}, but {@code TransportVersion} does not have separate major/minor version numbers.
 * So the minimum compatible version needs to be hard-coded as the transport version of the minimum compatible node version.
 * That variable should be updated appropriately whenever we do a major version release.
 */
public record TransportVersion(int id) implements Comparable<TransportVersion> {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerTransportVersion method.
     * When all the transport version constants have been registered, the map is cleared & never touched again.
     */
    private static Map<String, Integer> IDS = new HashMap<>();

    private static TransportVersion registerTransportVersion(int id, String uniqueId) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        Strings.requireNonEmpty(uniqueId, "Each TransportVersion needs a unique string id");
        Integer existing = IDS.put(uniqueId, id);
        if (existing != null) {
            throw new IllegalArgumentException("Versions " + id + " and " + existing + " have the same unique id");
        }
        return new TransportVersion(id);
    }

    public static final TransportVersion ZERO = registerTransportVersion(0, "00000000-0000-0000-0000-000000000000");
    public static final TransportVersion V_7_0_0 = registerTransportVersion(7_00_00_99, "7505fd05-d982-43ce-a63f-ff4c6c8bdeec");
    public static final TransportVersion V_7_0_1 = registerTransportVersion(7_00_01_99, "ae772780-e6f9-46a1-b0a0-20ed0cae37f7");
    public static final TransportVersion V_7_1_0 = registerTransportVersion(7_01_00_99, "fd09007c-1c54-450a-af99-9f941e1a53c2");
    public static final TransportVersion V_7_1_1 = registerTransportVersion(7_01_01_99, "f7ddb16c-3495-42ef-8d54-1461570ca68c");
    public static final TransportVersion V_7_2_0 = registerTransportVersion(7_02_00_99, "b74dbc52-e727-472c-af21-2156482e8796");
    public static final TransportVersion V_7_2_1 = registerTransportVersion(7_02_01_99, "a3217b94-f436-4aab-a020-162c83ba18f2");
    public static final TransportVersion V_7_3_0 = registerTransportVersion(7_03_00_99, "4f04e4c9-c5aa-49e4-8b99-abeb4e284a5a");
    public static final TransportVersion V_7_3_1 = registerTransportVersion(7_03_01_99, "532b9bc9-e11f-48a2-b997-67ca68ffb354");
    public static final TransportVersion V_7_3_2 = registerTransportVersion(7_03_02_99, "60da3953-8415-4d4f-a18d-853c3e68ebd6");
    public static final TransportVersion V_7_4_0 = registerTransportVersion(7_04_00_99, "ec7e58aa-55b4-4064-a9dd-fd723a2ba7a8");
    public static final TransportVersion V_7_4_1 = registerTransportVersion(7_04_01_99, "a316c26d-8e6a-4608-b1ec-062331552b98");
    public static final TransportVersion V_7_4_2 = registerTransportVersion(7_04_02_99, "031a77e1-3640-4c8a-80cf-28ded96bab48");
    public static final TransportVersion V_7_5_0 = registerTransportVersion(7_05_00_99, "cc6e14dc-9dc7-4b74-8e15-1f99a6cfbe03");
    public static final TransportVersion V_7_5_1 = registerTransportVersion(7_05_01_99, "9d12be44-16dc-44a8-a89a-45c9174ea596");
    public static final TransportVersion V_7_5_2 = registerTransportVersion(7_05_02_99, "484ed9de-7f5b-4e6b-a79a-0cb5e7570093");
    public static final TransportVersion V_7_6_0 = registerTransportVersion(7_06_00_99, "4637b8ae-f3df-43ae-a065-ad4c29f3373a");
    public static final TransportVersion V_7_6_1 = registerTransportVersion(7_06_01_99, "fe5b9f95-a311-4a92-943b-30ec256a331c");
    public static final TransportVersion V_7_6_2 = registerTransportVersion(7_06_02_99, "5396cb30-d91c-4789-85e8-77efd552c785");
    public static final TransportVersion V_7_7_0 = registerTransportVersion(7_07_00_99, "7bb73c48-ddb8-4437-b184-30371c35dd4b");
    public static final TransportVersion V_7_7_1 = registerTransportVersion(7_07_01_99, "85507b0f-0fca-4daf-a80b-451fe75e04a0");
    public static final TransportVersion V_7_8_0 = registerTransportVersion(7_08_00_99, "c3cc74af-d15e-494b-a907-6ad6dd2f4660");
    public static final TransportVersion V_7_8_1 = registerTransportVersion(7_08_01_99, "7acb9f6e-32f2-45ce-b87d-ca1f165b8e7a");
    public static final TransportVersion V_7_9_0 = registerTransportVersion(7_09_00_99, "9388fe76-192a-4053-b51c-d2a7b8eae545");
    public static final TransportVersion V_7_9_1 = registerTransportVersion(7_09_01_99, "30fa10fc-df6b-4435-bd9e-acdb9ae1b268");
    public static final TransportVersion V_7_9_2 = registerTransportVersion(7_09_02_99, "b58bb181-cecc-464e-b955-f6c1c1e7b4d0");
    public static final TransportVersion V_7_9_3 = registerTransportVersion(7_09_03_99, "4406926c-e2b6-4b9a-a72a-1bee8357ad3e");
    public static final TransportVersion V_7_10_0 = registerTransportVersion(7_10_00_99, "4efca195-38e4-4f74-b877-c26fb2a40733");
    public static final TransportVersion V_7_10_1 = registerTransportVersion(7_10_01_99, "0070260c-aa0b-4fc2-9c87-5cd5f23b005f");
    public static final TransportVersion V_7_10_2 = registerTransportVersion(7_10_02_99, "b369e2ed-261c-4b2f-8b42-0f0ba0549f8c");
    public static final TransportVersion V_7_11_0 = registerTransportVersion(7_11_00_99, "3b43bcbc-1c5e-4cc2-a3b4-8ac8b64239e8");
    public static final TransportVersion V_7_11_1 = registerTransportVersion(7_11_01_99, "2f75d13c-adde-4762-a46e-def8acce62b7");
    public static final TransportVersion V_7_11_2 = registerTransportVersion(7_11_02_99, "2c852a4b-236d-4e8b-9373-336c9b52685a");
    public static final TransportVersion V_7_12_0 = registerTransportVersion(7_12_00_99, "3be9ff6f-2d9f-4fc2-ba91-394dd5ebcf33");
    public static final TransportVersion V_7_12_1 = registerTransportVersion(7_12_01_99, "ee4fdfac-2039-4b00-b42d-579cbde7120c");
    public static final TransportVersion V_7_13_0 = registerTransportVersion(7_13_00_99, "e1fe494a-7c66-4571-8f8f-1d7e6d8df1b3");
    public static final TransportVersion V_7_13_1 = registerTransportVersion(7_13_01_99, "66bc8d82-36da-4d54-b22d-aca691dc3d70");
    public static final TransportVersion V_7_13_2 = registerTransportVersion(7_13_02_99, "2a6fc74c-4c44-4264-a619-37437cd2c5a0");
    public static final TransportVersion V_7_13_3 = registerTransportVersion(7_13_03_99, "a31592f5-f8d2-490c-a02e-da9501823d8d");
    public static final TransportVersion V_7_13_4 = registerTransportVersion(7_13_04_99, "3143240d-1831-4186-8a19-963336c4cea0");
    public static final TransportVersion V_7_14_0 = registerTransportVersion(7_14_00_99, "8cf0954c-b085-467f-b20b-3cb4b2e69e3e");
    public static final TransportVersion V_7_14_1 = registerTransportVersion(7_14_01_99, "3dbb62c3-cf73-4c76-8d5a-4ca70afe2c70");
    public static final TransportVersion V_7_14_2 = registerTransportVersion(7_14_02_99, "7943ae20-df60-45e5-97ba-82fc0dfc8b89");
    public static final TransportVersion V_7_15_0 = registerTransportVersion(7_15_00_99, "2273ac0e-00bb-4024-9e2e-ab78981623c6");
    public static final TransportVersion V_7_15_1 = registerTransportVersion(7_15_01_99, "a8c3503d-3452-45cf-b385-e855e16547fe");
    public static final TransportVersion V_7_15_2 = registerTransportVersion(7_15_02_99, "fbb8ad69-02e2-4c90-b2e4-23947107f8b4");
    public static final TransportVersion V_7_16_0 = registerTransportVersion(7_16_00_99, "59abadd2-25db-4547-a991-c92306a3934e");
    public static final TransportVersion V_7_16_1 = registerTransportVersion(7_16_01_99, "4ace6b6b-8bba-427f-8755-9e3b40092138");
    public static final TransportVersion V_7_16_2 = registerTransportVersion(7_16_02_99, "785567b9-b320-48ef-b538-1753228904cd");
    public static final TransportVersion V_7_16_3 = registerTransportVersion(7_16_03_99, "facf5ae7-3d4e-479c-9142-72529b784e30");
    public static final TransportVersion V_7_17_0 = registerTransportVersion(7_17_00_99, "322efe93-4c73-4e15-9274-bb76836c8fa8");
    public static final TransportVersion V_7_17_1 = registerTransportVersion(7_17_01_99, "51c72842-7974-4669-ad25-bf13ba307307");
    public static final TransportVersion V_7_17_2 = registerTransportVersion(7_17_02_99, "82bea8d0-bfea-47c2-b7d3-217d8feb67e3");
    public static final TransportVersion V_7_17_3 = registerTransportVersion(7_17_03_99, "a909c2f4-5cb8-46bf-af0f-cd18d1b7e9d2");
    public static final TransportVersion V_7_17_4 = registerTransportVersion(7_17_04_99, "5076e164-18a4-4373-8be7-15f1843c46db");
    public static final TransportVersion V_7_17_5 = registerTransportVersion(7_17_05_99, "da7e3509-7f61-4dd2-8d23-a61f628a62f6");
    public static final TransportVersion V_7_17_6 = registerTransportVersion(7_17_06_99, "a47ecf02-e457-474f-887d-ee15a7ebd969");
    public static final TransportVersion V_7_17_7 = registerTransportVersion(7_17_07_99, "108ba576-bb28-42f4-bcbf-845a0ce52560");
    public static final TransportVersion V_7_17_8 = registerTransportVersion(7_17_08_99, "82a3e70d-cf0e-4efb-ad16-6077ab9fe19f");
    public static final TransportVersion V_7_17_9 = registerTransportVersion(7_17_09_99, "afd50dda-735f-4eae-9309-3218ffec1b2d");
    public static final TransportVersion V_7_17_10 = registerTransportVersion(7_17_10_99, "18ae7108-6f7a-4205-adbb-cfcd6aa6ccc6");
    public static final TransportVersion V_7_17_11 = registerTransportVersion(7_17_11_99, "71c96c2a-e90b-4311-a4ac-23c453b075aa");
    public static final TransportVersion V_7_17_12 = registerTransportVersion(7_17_12_99, "379f6ed7-ea92-47db-b511-bb43ecdd7840");
    public static final TransportVersion V_8_0_0 = registerTransportVersion(8_00_00_99, "c7d2372c-9f01-4a79-8b11-227d862dfe4f");
    public static final TransportVersion V_8_0_1 = registerTransportVersion(8_00_01_99, "56e044c3-37e5-4f7e-bd38-f493927354ac");
    public static final TransportVersion V_8_1_0 = registerTransportVersion(8_01_00_99, "3dc49dce-9cef-492a-ac8d-3cc79f6b4280");
    public static final TransportVersion V_8_1_1 = registerTransportVersion(8_01_01_99, "40cf32e5-17b0-4187-9de1-022cdea69db9");
    public static final TransportVersion V_8_1_2 = registerTransportVersion(8_01_02_99, "54aa6394-08f3-4db7-b82e-314ae4b5b562");
    public static final TransportVersion V_8_1_3 = registerTransportVersion(8_01_03_99, "9772b54b-1e14-485f-92e8-8847b3a3d569");
    public static final TransportVersion V_8_2_0 = registerTransportVersion(8_02_00_99, "8ce6d555-202e-47db-ab7d-ade9dda1b7e8");
    public static final TransportVersion V_8_2_1 = registerTransportVersion(8_02_01_99, "ffbb67e8-cc33-4b02-a995-b461d9ee36c8");
    public static final TransportVersion V_8_2_2 = registerTransportVersion(8_02_02_99, "2499ee77-187d-4e10-8366-8e60d5f03676");
    public static final TransportVersion V_8_2_3 = registerTransportVersion(8_02_03_99, "046aae43-3090-4ece-8c27-8d489f097548");
    public static final TransportVersion V_8_3_0 = registerTransportVersion(8_03_00_99, "559ddb66-d857-4208-bed5-a995ccf478ea");
    public static final TransportVersion V_8_3_1 = registerTransportVersion(8_03_01_99, "31f9b136-dbbe-4fa1-b811-d6afa2a1b472");
    public static final TransportVersion V_8_3_2 = registerTransportVersion(8_03_02_99, "f6e9cd4c-2a71-4f9b-80d4-7ba97ebd18b2");
    public static final TransportVersion V_8_3_3 = registerTransportVersion(8_03_03_99, "a784de3e-533e-4844-8728-c55c6932dd8e");
    public static final TransportVersion V_8_4_0 = registerTransportVersion(8_04_00_99, "c0d12906-aa5b-45d4-94c7-cbcf4d9818ca");
    public static final TransportVersion V_8_4_1 = registerTransportVersion(8_04_01_99, "9a915f76-f259-4361-b53d-3f19c7797fd8");
    public static final TransportVersion V_8_4_2 = registerTransportVersion(8_04_02_99, "87c5b7b2-0f57-4172-8a81-b9f9a0198525");
    public static final TransportVersion V_8_4_3 = registerTransportVersion(8_04_03_99, "327cb1a0-9b5d-4be9-8033-285c2549f770");
    public static final TransportVersion V_8_5_0 = registerTransportVersion(8_05_00_99, "be3d7f23-7240-4904-9d7f-e25a0f766eca");
    public static final TransportVersion V_8_5_1 = registerTransportVersion(8_05_01_99, "d349d202-f01c-4dbb-85dd-947fb4267c99");
    public static final TransportVersion V_8_5_2 = registerTransportVersion(8_05_02_99, "b68b1331-fd64-44d9-9e71-f6796ec2024c");
    public static final TransportVersion V_8_5_3 = registerTransportVersion(8_05_03_99, "9ca3c835-e3b7-4622-a08e-d51e42403b06");
    public static final TransportVersion V_8_5_4 = registerTransportVersion(8_05_04_99, "97ee525c-555d-45ca-83dc-59cd592c8e86");
    public static final TransportVersion V_8_6_0 = registerTransportVersion(8_06_00_99, "e209c5ed-3488-4415-b561-33492ca3b789");
    public static final TransportVersion V_8_6_1 = registerTransportVersion(8_06_01_99, "9f113acb-1b21-4fda-bef9-2a3e669b5c7b");
    public static final TransportVersion V_8_6_2 = registerTransportVersion(8_06_02_99, "5a82fb68-b265-4a06-97c5-53496f823f51");
    public static final TransportVersion V_8_7_0 = registerTransportVersion(8_07_00_99, "f1ee7a85-4fa6-43f5-8679-33e2b750448b");
    public static final TransportVersion V_8_7_1 = registerTransportVersion(8_07_01_99, "018de9d8-9e8b-4ac7-8f4b-3a6fbd0487fb");
    public static final TransportVersion V_8_8_0 = registerTransportVersion(8_08_00_99, "f64fe576-0767-4ec3-984e-3e30b33b6c46");
    public static final TransportVersion V_8_8_1 = registerTransportVersion(8_08_01_99, "a177d86a-7a24-41a2-ade9-5235be957f3d");
    public static final TransportVersion V_8_8_2 = registerTransportVersion(8_08_02_99, "42f49370-f4f6-4dd6-b71e-a2b14de37982");
    public static final TransportVersion V_8_8_3 = registerTransportVersion(8_08_03_99, "b65ce268-3abc-45ad-bc54-c3bb1243b64f");
    /*
     * READ THE JAVADOC ABOVE BEFORE ADDING NEW TRANSPORT VERSIONS
     * Detached transport versions added below here. Starts at ES major version 10 equivalent.
     */
    // NOTE: DO NOT UNCOMMENT until all transport code uses TransportVersion
    // public static final TransportVersion V_10_000_000 = new TransportVersion(10_000_000, "dc3cbf06-3ed5-4e1b-9978-ee1d04d235bc");
    /*
     * When adding a new transport version, ensure there is a gap (say, 100) between versions
     * This is to make it possible to add intermediate versions for any bug fixes that may be required.
     *
     * When adding versions for patch fixes, add numbers in the middle of the gap. This is to ensure there is always some space
     * for patch fixes between any two versions.
     */

    static {
        // see comment on IDS field
        // now we're registered the transport versions, we can clear the map
        IDS = null;
    }

    /** Reference to the current transport version */
    public static final TransportVersion CURRENT = V_8_8_3;

    /** Reference to the earliest compatible transport version to this version of the codebase */
    // TODO: can we programmatically calculate or check this? Don't want to introduce circular ref between Version/TransportVersion
    public static final TransportVersion MINIMUM_COMPATIBLE = V_7_17_0;

    /**
     * Reference to the minimum transport version that can be used with CCS.
     * This should be the transport version used by the previous minor release.
     */
    public static final TransportVersion MINIMUM_CCS_VERSION = V_8_7_0;

    static NavigableMap<Integer, TransportVersion> getAllVersionIds(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        NavigableMap<Integer, TransportVersion> builder = new TreeMap<>();
        Map<String, TransportVersion> uniqueIds = new HashMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE", "MINIMUM_CCS_VERSION");
        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(TransportVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }
                try {
                    TransportVersion version = (TransportVersion) declaredField.get(null);

                    TransportVersion maybePrevious = builder.put(version.id, version);
                    assert maybePrevious == null
                        : "expected [" + version.id + "] to be uniquely mapped but saw [" + maybePrevious + "] and [" + version + "]";
                } catch (IllegalAccessException e) {
                    assert false : "Version field [" + fieldName + "] should be public";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, TransportVersion> VERSION_IDS;

    static {
        VERSION_IDS = getAllVersionIds(TransportVersion.class);
    }

    static Collection<TransportVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    public static TransportVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static TransportVersion fromId(int id) {
        TransportVersion known = VERSION_IDS.get(id);
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
        return version.onOrAfter(MINIMUM_COMPATIBLE);
    }

    public boolean after(TransportVersion version) {
        return version.id < id;
    }

    public boolean onOrAfter(TransportVersion version) {
        return version.id <= id;
    }

    public boolean before(TransportVersion version) {
        return version.id > id;
    }

    public boolean onOrBefore(TransportVersion version) {
        return version.id >= id;
    }

    public static TransportVersion fromString(String str) {
        return TransportVersion.fromId(Integer.parseInt(str));
    }

    @Override
    public int compareTo(TransportVersion other) {
        return Integer.compare(this.id, other.id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }
}
