/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.core.Assertions;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TransportVersions {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerTransportVersion method.
     * When all the transport version constants have been registered, the map is cleared & never touched again.
     */
    static TreeSet<Integer> IDS = new TreeSet<>();

    static TransportVersion def(int id) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        if (IDS.add(id) == false) {
            throw new IllegalArgumentException("Version id " + id + " defined twice");
        }
        if (id < IDS.last()) {
            throw new IllegalArgumentException("Version id " + id + " is not defined in the right location. Keep constants sorted");
        }
        return new TransportVersion(id);
    }

    public static final TransportVersion ZERO = def(0);
    public static final TransportVersion V_7_0_0 = def(7_00_00_99);
    public static final TransportVersion V_7_0_1 = def(7_00_01_99);
    public static final TransportVersion V_7_1_0 = def(7_01_00_99);
    public static final TransportVersion V_7_2_0 = def(7_02_00_99);
    public static final TransportVersion V_7_2_1 = def(7_02_01_99);
    public static final TransportVersion V_7_3_0 = def(7_03_00_99);
    public static final TransportVersion V_7_3_2 = def(7_03_02_99);
    public static final TransportVersion V_7_4_0 = def(7_04_00_99);
    public static final TransportVersion V_7_5_0 = def(7_05_00_99);
    public static final TransportVersion V_7_6_0 = def(7_06_00_99);
    public static final TransportVersion V_7_7_0 = def(7_07_00_99);
    public static final TransportVersion V_7_8_0 = def(7_08_00_99);
    public static final TransportVersion V_7_8_1 = def(7_08_01_99);
    public static final TransportVersion V_7_9_0 = def(7_09_00_99);
    public static final TransportVersion V_7_10_0 = def(7_10_00_99);
    public static final TransportVersion V_7_10_1 = def(7_10_01_99);
    public static final TransportVersion V_7_11_0 = def(7_11_00_99);
    public static final TransportVersion V_7_12_0 = def(7_12_00_99);
    public static final TransportVersion V_7_13_0 = def(7_13_00_99);
    public static final TransportVersion V_7_14_0 = def(7_14_00_99);
    public static final TransportVersion V_7_15_0 = def(7_15_00_99);
    public static final TransportVersion V_7_15_1 = def(7_15_01_99);
    public static final TransportVersion V_7_16_0 = def(7_16_00_99);
    public static final TransportVersion V_7_17_0 = def(7_17_00_99);
    public static final TransportVersion V_7_17_1 = def(7_17_01_99);
    public static final TransportVersion V_7_17_8 = def(7_17_08_99);
    public static final TransportVersion V_8_0_0 = def(8_00_00_99);
    public static final TransportVersion V_8_1_0 = def(8_01_00_99);
    public static final TransportVersion V_8_2_0 = def(8_02_00_99);
    public static final TransportVersion V_8_3_0 = def(8_03_00_99);
    public static final TransportVersion V_8_4_0 = def(8_04_00_99);
    public static final TransportVersion V_8_5_0 = def(8_05_00_99);
    public static final TransportVersion V_8_6_0 = def(8_06_00_99);
    public static final TransportVersion V_8_6_1 = def(8_06_01_99);
    public static final TransportVersion V_8_7_0 = def(8_07_00_99);
    public static final TransportVersion V_8_7_1 = def(8_07_01_99);
    public static final TransportVersion V_8_8_0 = def(8_08_00_99);
    public static final TransportVersion V_8_8_1 = def(8_08_01_99);
    /*
     * READ THE COMMENT BELOW THIS BLOCK OF DECLARATIONS BEFORE ADDING NEW TRANSPORT VERSIONS
     * Detached transport versions added below here.
     */
    public static final TransportVersion V_8_500_020 = def(8_500_020);
    public static final TransportVersion V_8_500_021 = def(8_500_021);
    public static final TransportVersion V_8_500_022 = def(8_500_022);
    public static final TransportVersion V_8_500_023 = def(8_500_023);
    public static final TransportVersion V_8_500_024 = def(8_500_024);
    public static final TransportVersion V_8_500_025 = def(8_500_025);
    public static final TransportVersion V_8_500_026 = def(8_500_026);
    public static final TransportVersion V_8_500_027 = def(8_500_027);
    public static final TransportVersion V_8_500_028 = def(8_500_028);
    public static final TransportVersion V_8_500_029 = def(8_500_029);
    public static final TransportVersion V_8_500_030 = def(8_500_030);
    public static final TransportVersion V_8_500_031 = def(8_500_031);
    public static final TransportVersion V_8_500_032 = def(8_500_032);
    public static final TransportVersion V_8_500_033 = def(8_500_033);
    public static final TransportVersion V_8_500_034 = def(8_500_034);
    public static final TransportVersion V_8_500_035 = def(8_500_035);
    public static final TransportVersion V_8_500_036 = def(8_500_036);
    public static final TransportVersion V_8_500_037 = def(8_500_037);
    public static final TransportVersion V_8_500_038 = def(8_500_038);
    public static final TransportVersion V_8_500_039 = def(8_500_039);
    public static final TransportVersion V_8_500_040 = def(8_500_040);
    public static final TransportVersion V_8_500_041 = def(8_500_041);
    public static final TransportVersion V_8_500_042 = def(8_500_042);
    public static final TransportVersion V_8_500_043 = def(8_500_043);
    public static final TransportVersion V_8_500_044 = def(8_500_044);
    public static final TransportVersion V_8_500_045 = def(8_500_045);
    public static final TransportVersion V_8_500_046 = def(8_500_046);
    public static final TransportVersion V_8_500_047 = def(8_500_047);
    public static final TransportVersion V_8_500_048 = def(8_500_048);
    public static final TransportVersion V_8_500_049 = def(8_500_049);
    public static final TransportVersion V_8_500_050 = def(8_500_050);
    public static final TransportVersion V_8_500_051 = def(8_500_051);
    public static final TransportVersion V_8_500_052 = def(8_500_052);
    public static final TransportVersion V_8_500_053 = def(8_500_053);
    public static final TransportVersion V_8_500_054 = def(8_500_054);
    public static final TransportVersion V_8_500_055 = def(8_500_055);
    public static final TransportVersion V_8_500_056 = def(8_500_056);
    public static final TransportVersion V_8_500_057 = def(8_500_057);
    public static final TransportVersion V_8_500_058 = def(8_500_058);
    public static final TransportVersion V_8_500_059 = def(8_500_059);
    public static final TransportVersion V_8_500_060 = def(8_500_060);
    public static final TransportVersion V_8_500_061 = def(8_500_061);
    public static final TransportVersion V_8_500_062 = def(8_500_062);
    public static final TransportVersion V_8_500_063 = def(8_500_063);
    public static final TransportVersion V_8_500_064 = def(8_500_064);
    public static final TransportVersion V_8_500_065 = def(8_500_065);
    public static final TransportVersion V_8_500_066 = def(8_500_066);
    public static final TransportVersion SEARCH_RESP_SKIP_UNAVAILABLE_ADDED = def(8_500_067);
    public static final TransportVersion ML_TRAINED_MODEL_FINISH_PENDING_WORK_ADDED = def(8_500_068);
    public static final TransportVersion SEARCH_APP_INDICES_REMOVED = def(8_500_069);
    public static final TransportVersion GENERIC_NAMED_WRITABLE_ADDED = def(8_500_070);
    public static final TransportVersion PINNED_QUERY_OPTIONAL_INDEX = def(8_500_071);
    public static final TransportVersion SHARD_SIZE_PRIMARY_TERM_GEN_ADDED = def(8_500_072);
    public static final TransportVersion COMPAT_VERSIONS_MAPPING_VERSION_ADDED = def(8_500_073);
    public static final TransportVersion V_8_500_074 = def(8_500_074);
    public static final TransportVersion NODE_INFO_INDEX_VERSION_ADDED = def(8_500_075);
    public static final TransportVersion FIRST_NEW_ID_LAYOUT = def(8_501_00_0);
    public static final TransportVersion COMMIT_PRIMARY_TERM_GENERATION = def(8_501_00_1);
    public static final TransportVersion WAIT_FOR_CLUSTER_STATE_IN_RECOVERY_ADDED = def(8_502_00_0);
    public static final TransportVersion RECOVERY_COMMIT_TOO_NEW_EXCEPTION_ADDED = def(8_503_00_0);
    public static final TransportVersion NODE_INFO_COMPONENT_VERSIONS_ADDED = def(8_504_00_0);
    public static final TransportVersion COMPACT_FIELD_CAPS_ADDED = def(8_505_00_0);
    public static final TransportVersion DATA_STREAM_RESPONSE_INDEX_PROPERTIES = def(8_506_00_0);
    public static final TransportVersion ML_TRAINED_MODEL_CONFIG_PLATFORM_ADDED = def(8_507_00_0);

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
     * ADDING A TRANSPORT VERSION
     * To add a new transport version, add a new constant at the bottom of the list, above this comment. Don't add other lines,
     * comments, etc. The version id has the following layout:
     *
     * M_NNN_SS_P
     *
     * M - The major version of Elasticsearch
     * NNN - The server version part
     * SS - The serverless version part. It should always be 00 here, it is used by serverless only.
     * P - The patch version part
     *
     * To determine the id of the next TransportVersion constant, do the following:
     * - Use the same major version, unless bumping majors
     * - Bump the server version part by 1, unless creating a patch version
     * - Leave the serverless part as 00
     * - Bump the patch part if creating a patch version
     *
     * If a patch version is created, it should be placed sorted among the other existing constants.
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

    public static NavigableMap<Integer, TransportVersion> getAllVersionIds(Class<?> cls) {
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
