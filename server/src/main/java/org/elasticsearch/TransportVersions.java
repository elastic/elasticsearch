/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.UpdateForV9;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntFunction;

/**
 * <p>Transport version is used to coordinate compatible wire protocol communication between nodes, at a fine-grained level.  This replaces
 * and supersedes the old Version constants.</p>
 *
 * <p>Before adding a new version constant, please read the block comment at the end of the list of constants.</p>
 */
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

    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA) // remove the transport versions with which v9 will not need to interact
    public static final TransportVersion ZERO = def(0);
    public static final TransportVersion V_7_0_0 = def(7_00_00_99);
    public static final TransportVersion V_7_3_0 = def(7_03_00_99);
    public static final TransportVersion V_7_4_0 = def(7_04_00_99);
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
    public static final TransportVersion V_8_9_X = def(8_500_020);
    public static final TransportVersion V_8_10_X = def(8_500_061);
    public static final TransportVersion V_8_11_X = def(8_512_00_1);
    public static final TransportVersion V_8_12_0 = def(8_560_00_0);
    public static final TransportVersion V_8_12_1 = def(8_560_00_1);
    public static final TransportVersion V_8_13_0 = def(8_595_00_0);
    public static final TransportVersion V_8_13_4 = def(8_595_00_1);
    public static final TransportVersion V_8_14_0 = def(8_636_00_1);
    public static final TransportVersion V_8_15_0 = def(8_702_00_2);
    public static final TransportVersion V_8_15_2 = def(8_702_00_3);
    public static final TransportVersion V_8_16_0 = def(8_772_00_1);
    public static final TransportVersion ADD_COMPATIBILITY_VERSIONS_TO_NODE_INFO_BACKPORT_8_16 = def(8_772_00_2);
    public static final TransportVersion SKIP_INNER_HITS_SEARCH_SOURCE_BACKPORT_8_16 = def(8_772_00_3);
    public static final TransportVersion QUERY_RULES_LIST_INCLUDES_TYPES_BACKPORT_8_16 = def(8_772_00_4);
    public static final TransportVersion REMOVE_MIN_COMPATIBLE_SHARD_NODE = def(8_773_00_0);
    public static final TransportVersion REVERT_REMOVE_MIN_COMPATIBLE_SHARD_NODE = def(8_774_00_0);
    public static final TransportVersion ESQL_FIELD_ATTRIBUTE_PARENT_SIMPLIFIED = def(8_775_00_0);
    public static final TransportVersion INFERENCE_DONT_PERSIST_ON_READ = def(8_776_00_0);
    public static final TransportVersion SIMULATE_MAPPING_ADDITION = def(8_777_00_0);
    public static final TransportVersion INTRODUCE_ALL_APPLICABLE_SELECTOR = def(8_778_00_0);
    public static final TransportVersion INDEX_MODE_LOOKUP = def(8_779_00_0);
    public static final TransportVersion INDEX_REQUEST_REMOVE_METERING = def(8_780_00_0);
    public static final TransportVersion CPU_STAT_STRING_PARSING = def(8_781_00_0);
    public static final TransportVersion QUERY_RULES_RETRIEVER = def(8_782_00_0);
    public static final TransportVersion ESQL_CCS_EXEC_INFO_WITH_FAILURES = def(8_783_00_0);
    public static final TransportVersion LOGSDB_TELEMETRY = def(8_784_00_0);
    public static final TransportVersion LOGSDB_TELEMETRY_STATS = def(8_785_00_0);
    public static final TransportVersion KQL_QUERY_ADDED = def(8_786_00_0);
    public static final TransportVersion ROLE_MONITOR_STATS = def(8_787_00_0);
    public static final TransportVersion DATA_STREAM_INDEX_VERSION_DEPRECATION_CHECK = def(8_788_00_0);
    public static final TransportVersion ADD_COMPATIBILITY_VERSIONS_TO_NODE_INFO = def(8_789_00_0);
    public static final TransportVersion VERTEX_AI_INPUT_TYPE_ADDED = def(8_790_00_0);
    public static final TransportVersion SKIP_INNER_HITS_SEARCH_SOURCE = def(8_791_00_0);
    public static final TransportVersion QUERY_RULES_LIST_INCLUDES_TYPES = def(8_792_00_0);
    public static final TransportVersion INDEX_STATS_ADDITIONAL_FIELDS = def(8_793_00_0);
    public static final TransportVersion INDEX_STATS_ADDITIONAL_FIELDS_REVERT = def(8_794_00_0);
    public static final TransportVersion FAST_REFRESH_RCO_2 = def(8_795_00_0);
    public static final TransportVersion ESQL_ENRICH_RUNTIME_WARNINGS = def(8_796_00_0);
    public static final TransportVersion INGEST_PIPELINE_CONFIGURATION_AS_MAP = def(8_797_00_0);
    public static final TransportVersion LOGSDB_TELEMETRY_CUSTOM_CUTOFF_DATE_FIX_8_17 = def(8_797_00_1);
    public static final TransportVersion SOURCE_MODE_TELEMETRY_FIX_8_17 = def(8_797_00_2);
    public static final TransportVersion INDEXING_PRESSURE_THROTTLING_STATS = def(8_798_00_0);
    public static final TransportVersion REINDEX_DATA_STREAMS = def(8_799_00_0);
    public static final TransportVersion ESQL_REMOVE_NODE_LEVEL_PLAN = def(8_800_00_0);
    public static final TransportVersion LOGSDB_TELEMETRY_CUSTOM_CUTOFF_DATE = def(8_801_00_0);
    public static final TransportVersion SOURCE_MODE_TELEMETRY = def(8_802_00_0);
    public static final TransportVersion NEW_REFRESH_CLUSTER_BLOCK = def(8_803_00_0);
    public static final TransportVersion RETRIES_AND_OPERATIONS_IN_BLOBSTORE_STATS = def(8_804_00_0);
    public static final TransportVersion ADD_DATA_STREAM_OPTIONS_TO_TEMPLATES = def(8_805_00_0);
    public static final TransportVersion KNN_QUERY_RESCORE_OVERSAMPLE = def(8_806_00_0);
    public static final TransportVersion SEMANTIC_QUERY_LENIENT = def(8_807_00_0);

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
     *     git show v8.11.0:server/src/main/java/org/elasticsearch/TransportVersions.java | grep '= def'
     *
     * ... or by a particular branch ...
     *
     *     git show 8.11:server/src/main/java/org/elasticsearch/TransportVersions.java | grep '= def'
     *
     * ... and you can see which versions were added in between two versions too ...
     *
     *     git diff v8.11.0..main -- server/src/main/java/org/elasticsearch/TransportVersions.java
     *
     * In branches 8.7-8.10 see server/src/main/java/org/elasticsearch/TransportVersion.java for the equivalent definitions.
     */

    /**
     * Reference to the earliest compatible transport version to this version of the codebase.
     * This should be the transport version used by the highest minor version of the previous major.
     */
    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA)
    // This needs to be bumped to the 8.last
    public static final TransportVersion MINIMUM_COMPATIBLE = V_7_17_0;

    /**
     * Reference to the minimum transport version that can be used with CCS.
     * This should be the transport version used by the previous minor release.
     */
    public static final TransportVersion MINIMUM_CCS_VERSION = V_8_15_0;

    /**
     * Sorted list of all versions defined in this class
     */
    static final List<TransportVersion> DEFINED_VERSIONS = collectAllVersionIdsDefinedInClass(TransportVersions.class);

    // the highest transport version constant defined
    static final TransportVersion LATEST_DEFINED;
    static {
        LATEST_DEFINED = DEFINED_VERSIONS.getLast();

        // see comment on IDS field
        // now we're registered all the transport versions, we can clear the map
        IDS = null;
    }

    public static List<TransportVersion> collectAllVersionIdsDefinedInClass(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        List<TransportVersion> definedTransportVersions = new ArrayList<>();

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
                definedTransportVersions.add(version);

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

        Collections.sort(definedTransportVersions);

        return List.copyOf(definedTransportVersions);
    }

    static final IntFunction<String> VERSION_LOOKUP = ReleaseVersions.generateVersionsLookup(TransportVersions.class, LATEST_DEFINED.id());

    // no instance
    private TransportVersions() {}
}
