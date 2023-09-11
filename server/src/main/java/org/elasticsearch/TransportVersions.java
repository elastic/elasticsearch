/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Assertions;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class TransportVersions {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerTransportVersion method.
     * When all the transport version constants have been registered, the map is cleared & never touched again.
     */
    static Map<String, Integer> IDS = new HashMap<>();

    static TransportVersion def(int id, String uniqueId) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        Strings.requireNonEmpty(uniqueId, "Each TransportVersion needs a unique string id");
        Integer existing = IDS.put(uniqueId, id);
        if (existing != null) {
            throw new IllegalArgumentException("Versions " + id + " and " + existing + " have the same unique id");
        }
        return new TransportVersion(id);
    }

    public static final TransportVersion ZERO = def(0, "00000000-0000-0000-0000-000000000000");
    public static final TransportVersion V_7_0_0 = def(7_00_00_99, "7505fd05-d982-43ce-a63f-ff4c6c8bdeec");
    public static final TransportVersion V_7_0_1 = def(7_00_01_99, "ae772780-e6f9-46a1-b0a0-20ed0cae37f7");
    public static final TransportVersion V_7_1_0 = def(7_01_00_99, "fd09007c-1c54-450a-af99-9f941e1a53c2");
    public static final TransportVersion V_7_2_0 = def(7_02_00_99, "b74dbc52-e727-472c-af21-2156482e8796");
    public static final TransportVersion V_7_2_1 = def(7_02_01_99, "a3217b94-f436-4aab-a020-162c83ba18f2");
    public static final TransportVersion V_7_3_0 = def(7_03_00_99, "4f04e4c9-c5aa-49e4-8b99-abeb4e284a5a");
    public static final TransportVersion V_7_3_2 = def(7_03_02_99, "60da3953-8415-4d4f-a18d-853c3e68ebd6");
    public static final TransportVersion V_7_4_0 = def(7_04_00_99, "ec7e58aa-55b4-4064-a9dd-fd723a2ba7a8");
    public static final TransportVersion V_7_5_0 = def(7_05_00_99, "cc6e14dc-9dc7-4b74-8e15-1f99a6cfbe03");
    public static final TransportVersion V_7_6_0 = def(7_06_00_99, "4637b8ae-f3df-43ae-a065-ad4c29f3373a");
    public static final TransportVersion V_7_7_0 = def(7_07_00_99, "7bb73c48-ddb8-4437-b184-30371c35dd4b");
    public static final TransportVersion V_7_8_0 = def(7_08_00_99, "c3cc74af-d15e-494b-a907-6ad6dd2f4660");
    public static final TransportVersion V_7_8_1 = def(7_08_01_99, "7acb9f6e-32f2-45ce-b87d-ca1f165b8e7a");
    public static final TransportVersion V_7_9_0 = def(7_09_00_99, "9388fe76-192a-4053-b51c-d2a7b8eae545");
    public static final TransportVersion V_7_10_0 = def(7_10_00_99, "4efca195-38e4-4f74-b877-c26fb2a40733");
    public static final TransportVersion V_7_10_1 = def(7_10_01_99, "0070260c-aa0b-4fc2-9c87-5cd5f23b005f");
    public static final TransportVersion V_7_11_0 = def(7_11_00_99, "3b43bcbc-1c5e-4cc2-a3b4-8ac8b64239e8");
    public static final TransportVersion V_7_12_0 = def(7_12_00_99, "3be9ff6f-2d9f-4fc2-ba91-394dd5ebcf33");
    public static final TransportVersion V_7_13_0 = def(7_13_00_99, "e1fe494a-7c66-4571-8f8f-1d7e6d8df1b3");
    public static final TransportVersion V_7_14_0 = def(7_14_00_99, "8cf0954c-b085-467f-b20b-3cb4b2e69e3e");
    public static final TransportVersion V_7_15_0 = def(7_15_00_99, "2273ac0e-00bb-4024-9e2e-ab78981623c6");
    public static final TransportVersion V_7_15_1 = def(7_15_01_99, "a8c3503d-3452-45cf-b385-e855e16547fe");
    public static final TransportVersion V_7_16_0 = def(7_16_00_99, "59abadd2-25db-4547-a991-c92306a3934e");
    public static final TransportVersion V_7_17_0 = def(7_17_00_99, "322efe93-4c73-4e15-9274-bb76836c8fa8");
    public static final TransportVersion V_7_17_1 = def(7_17_01_99, "51c72842-7974-4669-ad25-bf13ba307307");
    public static final TransportVersion V_7_17_8 = def(7_17_08_99, "82a3e70d-cf0e-4efb-ad16-6077ab9fe19f");
    public static final TransportVersion V_8_0_0 = def(8_00_00_99, "c7d2372c-9f01-4a79-8b11-227d862dfe4f");
    public static final TransportVersion V_8_1_0 = def(8_01_00_99, "3dc49dce-9cef-492a-ac8d-3cc79f6b4280");
    public static final TransportVersion V_8_2_0 = def(8_02_00_99, "8ce6d555-202e-47db-ab7d-ade9dda1b7e8");
    public static final TransportVersion V_8_3_0 = def(8_03_00_99, "559ddb66-d857-4208-bed5-a995ccf478ea");
    public static final TransportVersion V_8_4_0 = def(8_04_00_99, "c0d12906-aa5b-45d4-94c7-cbcf4d9818ca");
    public static final TransportVersion V_8_5_0 = def(8_05_00_99, "be3d7f23-7240-4904-9d7f-e25a0f766eca");
    public static final TransportVersion V_8_6_0 = def(8_06_00_99, "e209c5ed-3488-4415-b561-33492ca3b789");
    public static final TransportVersion V_8_6_1 = def(8_06_01_99, "9f113acb-1b21-4fda-bef9-2a3e669b5c7b");
    public static final TransportVersion V_8_7_0 = def(8_07_00_99, "f1ee7a85-4fa6-43f5-8679-33e2b750448b");
    public static final TransportVersion V_8_7_1 = def(8_07_01_99, "018de9d8-9e8b-4ac7-8f4b-3a6fbd0487fb");
    public static final TransportVersion V_8_8_0 = def(8_08_00_99, "f64fe576-0767-4ec3-984e-3e30b33b6c46");
    public static final TransportVersion V_8_8_1 = def(8_08_01_99, "291c71bb-5b0a-4b7e-a407-6e53bc128d0f");
    /*
     * READ THE COMMENT BELOW THIS BLOCK OF DECLARATIONS BEFORE ADDING NEW TRANSPORT VERSIONS
     * Detached transport versions added below here.
     */
    public static final TransportVersion V_8_500_020 = def(8_500_020, "ECB42C26-B258-42E5-A835-E31AF84A76DE");
    public static final TransportVersion V_8_500_021 = def(8_500_021, "102e0d84-0c08-402c-a696-935f3a3da873");
    public static final TransportVersion V_8_500_022 = def(8_500_022, "4993c724-7a81-4955-84e7-403484610091");
    public static final TransportVersion V_8_500_023 = def(8_500_023, "01b06435-5d73-42ff-a121-3b36b771375e");
    public static final TransportVersion V_8_500_024 = def(8_500_024, "db337007-f823-4dbd-968e-375383814c17");
    public static final TransportVersion V_8_500_025 = def(8_500_025, "b2ab7b75-5ac2-4a3b-bbb6-8789ca66722d");
    public static final TransportVersion V_8_500_026 = def(8_500_026, "965d294b-14aa-4abb-bcfc-34631187941d");
    public static final TransportVersion V_8_500_027 = def(8_500_027, "B151D967-8E7C-401C-8275-0ABC06335F2D");
    public static final TransportVersion V_8_500_028 = def(8_500_028, "a6592d08-15cb-4e1a-b9b4-b2ba24058444");
    public static final TransportVersion V_8_500_029 = def(8_500_029, "f3bd98af-6187-e161-e315-718a2fecc2db");
    public static final TransportVersion V_8_500_030 = def(8_500_030, "b72d7f12-8ed3-4a5b-8e6a-4910ea10e0d7");
    public static final TransportVersion V_8_500_031 = def(8_500_031, "e7aa7e95-37e7-46a3-aad1-90a21c0769e7");
    public static final TransportVersion V_8_500_032 = def(8_500_032, "a9a14bc6-c3f2-41d9-a3d8-c686bf2c901d");
    public static final TransportVersion V_8_500_033 = def(8_500_033, "193ab7c4-a751-4cbd-a66a-2d7d56ccbc10");
    public static final TransportVersion V_8_500_034 = def(8_500_034, "16871c8b-88ba-4432-980a-10fd9ecad2dc");
    public static final TransportVersion V_8_500_035 = def(8_500_035, "664dd6ce-3487-4fbd-81a9-af778b28be45");
    public static final TransportVersion V_8_500_036 = def(8_500_036, "3343c64f-d7ac-4f02-9262-3e1acfc56f89");
    public static final TransportVersion V_8_500_037 = def(8_500_037, "d76a4f22-8878-43e0-acfa-15e452195fa7");
    public static final TransportVersion V_8_500_038 = def(8_500_038, "9ef93580-feae-409f-9989-b49e411ca7a9");
    public static final TransportVersion V_8_500_039 = def(8_500_039, "c23722d7-6139-4cf2-b8a1-600fbd4ec359");
    public static final TransportVersion V_8_500_040 = def(8_500_040, "8F3AA068-A608-4A16-9683-2412A75BF2DD");
    public static final TransportVersion V_8_500_041 = def(8_500_041, "5b6a0fd0-ac0b-443f-baae-cffec140905c");
    public static final TransportVersion V_8_500_042 = def(8_500_042, "763b4801-a4fc-47c4-aff5-7f5a757b8a07");
    public static final TransportVersion V_8_500_043 = def(8_500_043, "50babd14-7f5c-4f8c-9351-94e0d397aabc");
    public static final TransportVersion V_8_500_044 = def(8_500_044, "96b83320-2317-4e9d-b735-356f18c1d76a");
    public static final TransportVersion V_8_500_045 = def(8_500_045, "24a596dd-c843-4c0a-90b3-759697d74026");
    public static final TransportVersion V_8_500_046 = def(8_500_046, "61666d4c-a4f0-40db-8a3d-4806718247c5");
    public static final TransportVersion V_8_500_047 = def(8_500_047, "4b1682fe-c37e-4184-80f6-7d57fcba9b3d");
    public static final TransportVersion V_8_500_048 = def(8_500_048, "f9658aa5-f066-4edb-bcb9-40bf256c9294");
    public static final TransportVersion V_8_500_049 = def(8_500_049, "828bb6ce-2fbb-11ee-be56-0242ac120002");
    public static final TransportVersion V_8_500_050 = def(8_500_050, "69722fa2-7c0a-4227-86fb-6d6a9a0a0321");
    public static final TransportVersion V_8_500_051 = def(8_500_051, "a28b43bc-bb5f-4406-afcf-26900aa98a71");
    public static final TransportVersion V_8_500_052 = def(8_500_052, "2d382b3d-9838-4cce-84c8-4142113e5c2b");
    public static final TransportVersion V_8_500_053 = def(8_500_053, "aa603bae-01e2-380a-8950-6604468e8c6d");
    public static final TransportVersion V_8_500_054 = def(8_500_054, "b76ef950-af03-4dda-85c2-6400ec442e7e");
    public static final TransportVersion V_8_500_055 = def(8_500_055, "7831c609-0df1-42d6-aa97-8a346c389ef");
    public static final TransportVersion V_8_500_056 = def(8_500_056, "afa8c4be-29c9-48ab-b1ed-7182415c1b71");
    public static final TransportVersion V_8_500_057 = def(8_500_057, "80c088c6-358d-43b2-8d9c-1ea3c6c2b9fd");
    public static final TransportVersion V_8_500_058 = def(8_500_058, "41d9c98a-1de2-4dc1-86f1-abd4cc1bef57");
    public static final TransportVersion V_8_500_059 = def(8_500_059, "2f2090c0-7cd0-4a10-8f02-63d26073604f");
    public static final TransportVersion V_8_500_060 = def(8_500_060, "ec065a44-b468-4f8a-aded-7b90ca8d792b");
    public static final TransportVersion V_8_500_061 = def(8_500_061, "4e07f830-8be4-448c-851e-62b3d2f0bf0a");
    public static final TransportVersion V_8_500_062 = def(8_500_062, "09CD9C9B-3207-4B40-8756-B7A12001A885");
    public static final TransportVersion V_8_500_063 = def(8_500_063, "31dedced-0055-4f34-b952-2f6919be7488");
    public static final TransportVersion V_8_500_064 = def(8_500_064, "3a795175-5e6f-40ff-90fe-5571ea8ab04e");
    public static final TransportVersion V_8_500_065 = def(8_500_065, "4e253c58-1b3d-11ee-be56-0242ac120002");
    public static final TransportVersion V_8_500_066 = def(8_500_066, "F398ECC6-5D2A-4BD8-A9E8-1101F030DF85");
    public static final TransportVersion V_8_500_067 = def(8_500_067, "a7c86604-a917-4aff-9a1b-a4d44c3dbe02");
    public static final TransportVersion V_8_500_068 = def(8_500_068, "2683c8b4-5372-4a6a-bb3a-d61aa679089a");
    public static final TransportVersion V_8_500_069 = def(8_500_069, "5b804027-d8a0-421b-9970-1f53d766854b");
    public static final TransportVersion V_8_500_070 = def(8_500_070, "6BADC9CD-3C9D-4381-8BD9-B305CAA93F86");
    public static final TransportVersion V_8_500_071 = def(8_500_071, "a86dfc08-3026-4f01-90ef-6d6de003e217");
    public static final TransportVersion V_8_500_072 = def(8_500_072, "e2df7d80-7b74-4afd-9734-aee0fc256025");
    public static final TransportVersion V_8_500_073 = def(8_500_073, "9128e16a-e4f7-41c4-b04f-842955bfc1b4");
    /*
     * STOP! READ THIS FIRST! No, really,
     *        ____ _____ ___  ____  _        ____  _____    _    ____    _____ _   _ ___ ____    _____ ___ ____  ____ _____ _
     *       / ___|_   _/ _ \|  _ \| |      |  _ \| ____|  / \  |  _ \  |_   _| | | |_ _/ ___|  |  ___|_ _|  _ \/ ___|_   _| |
     *       \___ \ | || | | | |_) | |      | |_) |  _|   / _ \ | | | |   | | | |_| || |\___ \  | |_   | || |_) \___ \ | | | |
     *        ___) || || |_| |  __/|_|      |  _ <| |___ / ___ \| |_| |   | | |  _  || | ___) | |  _|  | ||  _ < ___) || | |_|
     *       |____/ |_| \___/|_|   (_)      |_| \_\_____/_/   \_\____/    |_| |_| |_|___|____/  |_|   |___|_| \_\____/ |_| (_)
     *
     * A new transport version should be added EVERY TIME a change is made to the serialization protocol of one or more classes. Each
     * transport version should only be used in a single merged commit (apart from the BwC versions copied from o.e.Version, ≤V_8_8_1).
     *
     * To add a new transport version, add a new constant at the bottom of the list, above this comment, which is one greater than the
     * current highest version and ensure it has a fresh UUID. Don't add other lines, comments, etc.
     *
     * REVERTING A TRANSPORT VERSION
     *
     * If you revert a commit with a transport version change, you MUST ensure there is a NEW transport version representing the reverted
     * change. DO NOT let the transport version go backwards, it must ALWAYS be incremented.
     *
     * DETERMINING TRANSPORT VERSIONS FROM GIT HISTORY
     *
     * If your git checkout has the expected minor-version-numbered branches and the expected release-version tags then you can find the
     * transport versions known by a particular release ...
     *
     *     git show v8.9.1:server/src/main/java/org/elasticsearch/TransportVersions.java | grep def
     *
     * ... or by a particular branch ...
     *
     *     git show 8.10:server/src/main/java/org/elasticsearch/TransportVersions.java | grep def
     *
     * ... and you can see which versions were added in between two versions too ...
     *
     *     git diff 8.10..main -- server/src/main/java/org/elasticsearch/TransportVersions.java
     */

    /**
     * Reference to the earliest compatible transport version to this version of the codebase.
     * This should be the transport version used by the highest minor version of the previous major.
     */
    public static final TransportVersion MINIMUM_COMPATIBLE = V_7_17_0;

    /**
     * Reference to the minimum transport version that can be used with CCS.
     * This should be the transport version used by the previous minor release.
     */
    public static final TransportVersion MINIMUM_CCS_VERSION = V_8_500_061;

    static final NavigableMap<Integer, TransportVersion> VERSION_IDS = getAllVersionIds(TransportVersions.class);

    // the highest transport version constant defined in this file, used as a fallback for TransportVersion.current()
    static final TransportVersion LATEST_DEFINED;
    static {
        LATEST_DEFINED = VERSION_IDS.lastEntry().getValue();

        // see comment on IDS field
        // now we're registered all the transport versions, we can clear the map
        IDS = null;
    }

    static NavigableMap<Integer, TransportVersion> getAllVersionIds(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        NavigableMap<Integer, TransportVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE", "MINIMUM_CCS_VERSION");

        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(TransportVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }

                TransportVersion version;
                try {
                    version = (TransportVersion) declaredField.get(null);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
                builder.put(version.id(), version);

                if (Assertions.ENABLED) {
                    // check the version number is unique
                    var sameVersionNumber = versionIdFields.put(version.id(), fieldName);
                    assert sameVersionNumber == null
                        : "Versions ["
                            + sameVersionNumber
                            + "] and ["
                            + fieldName
                            + "] have the same version number ["
                            + version.id()
                            + "]. Each TransportVersion should have a different version number";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    static Collection<TransportVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    // no instance
    private TransportVersions() {}
}
