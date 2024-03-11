/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.UpdateForV9;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
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

    @UpdateForV9 // remove the transport versions with which v9 will not need to interact
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
    public static final TransportVersion V_8_9_X = def(8_500_020);
    public static final TransportVersion V_8_10_X = def(8_500_061);
    public static final TransportVersion V_8_11_X = def(8_512_00_1);
    public static final TransportVersion V_8_12_0 = def(8_560_00_0);
    public static final TransportVersion DATE_HISTOGRAM_SUPPORT_DOWNSAMPLED_TZ_8_12_PATCH = def(8_560_00_1);
    public static final TransportVersion NODE_STATS_REQUEST_SIMPLIFIED = def(8_561_00_0);
    public static final TransportVersion TEXT_EXPANSION_TOKEN_PRUNING_CONFIG_ADDED = def(8_562_00_0);
    public static final TransportVersion ESQL_ASYNC_QUERY = def(8_563_00_0);
    public static final TransportVersion ESQL_STATUS_INCLUDE_LUCENE_QUERIES = def(8_564_00_0);
    public static final TransportVersion ESQL_CLUSTER_ALIAS = def(8_565_00_0);
    public static final TransportVersion SNAPSHOTS_IN_PROGRESS_TRACKING_REMOVING_NODES_ADDED = def(8_566_00_0);
    public static final TransportVersion SMALLER_RELOAD_SECURE_SETTINGS_REQUEST = def(8_567_00_0);
    public static final TransportVersion UPDATE_API_KEY_EXPIRATION_TIME_ADDED = def(8_568_00_0);
    public static final TransportVersion LAZY_ROLLOVER_ADDED = def(8_569_00_0);
    public static final TransportVersion ESQL_PLAN_POINT_LITERAL_WKB = def(8_570_00_0);
    public static final TransportVersion HOT_THREADS_AS_BYTES = def(8_571_00_0);
    public static final TransportVersion ML_INFERENCE_REQUEST_INPUT_TYPE_ADDED = def(8_572_00_0);
    public static final TransportVersion ESQL_ENRICH_POLICY_CCQ_MODE = def(8_573_00_0);
    public static final TransportVersion DATE_HISTOGRAM_SUPPORT_DOWNSAMPLED_TZ = def(8_574_00_0);
    public static final TransportVersion PEERFINDER_REPORTS_PEERS_MASTERS = def(8_575_00_0);
    public static final TransportVersion ESQL_MULTI_CLUSTERS_ENRICH = def(8_576_00_0);
    public static final TransportVersion NESTED_KNN_MORE_INNER_HITS = def(8_577_00_0);
    public static final TransportVersion REQUIRE_DATA_STREAM_ADDED = def(8_578_00_0);
    public static final TransportVersion ML_INFERENCE_COHERE_EMBEDDINGS_ADDED = def(8_579_00_0);
    public static final TransportVersion DESIRED_NODE_VERSION_OPTIONAL_STRING = def(8_580_00_0);
    public static final TransportVersion ML_INFERENCE_REQUEST_INPUT_TYPE_UNSPECIFIED_ADDED = def(8_581_00_0);
    public static final TransportVersion ASYNC_SEARCH_STATUS_SUPPORTS_KEEP_ALIVE = def(8_582_00_0);
    public static final TransportVersion KNN_QUERY_NUMCANDS_AS_OPTIONAL_PARAM = def(8_583_00_0);
    public static final TransportVersion TRANSFORM_GET_BASIC_STATS = def(8_584_00_0);
    public static final TransportVersion NLP_DOCUMENT_CHUNKING_ADDED = def(8_585_00_0);
    public static final TransportVersion SEARCH_TIMEOUT_EXCEPTION_ADDED = def(8_586_00_0);
    public static final TransportVersion ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED = def(8_587_00_0);
    public static final TransportVersion HEALTH_INFO_ENRICHED_WITH_REPOS = def(8_588_00_0);
    public static final TransportVersion RESOLVE_CLUSTER_ENDPOINT_ADDED = def(8_589_00_0);
    public static final TransportVersion FIELD_CAPS_FIELD_HAS_VALUE = def(8_590_00_0);
    public static final TransportVersion ML_INFERENCE_REQUEST_INPUT_TYPE_CLASS_CLUSTER_ADDED = def(8_591_00_0);
    public static final TransportVersion ML_DIMENSIONS_SET_BY_USER_ADDED = def(8_592_00_0);
    public static final TransportVersion INDEX_REQUEST_NORMALIZED_BYTES_PARSED = def(8_593_00_0);
    public static final TransportVersion INGEST_GRAPH_STRUCTURE_EXCEPTION = def(8_594_00_0);
    public static final TransportVersion ML_MODEL_IN_SERVICE_SETTINGS = def(8_595_00_0);
    public static final TransportVersion RANDOM_AGG_SHARD_SEED = def(8_596_00_0);
    public static final TransportVersion ESQL_TIMINGS = def(8_597_00_0);
    public static final TransportVersion DATA_STREAM_AUTO_SHARDING_EVENT = def(8_598_00_0);
    public static final TransportVersion ADD_FAILURE_STORE_INDICES_OPTIONS = def(8_599_00_0);
    public static final TransportVersion ESQL_ENRICH_OPERATOR_STATUS = def(8_600_00_0);
    public static final TransportVersion ESQL_SERIALIZE_ARRAY_VECTOR = def(8_601_00_0);
    public static final TransportVersion ESQL_SERIALIZE_ARRAY_BLOCK = def(8_602_00_0);
    public static final TransportVersion ADD_DATA_STREAM_GLOBAL_RETENTION = def(8_603_00_0);

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
    public static final TransportVersion MINIMUM_COMPATIBLE = V_7_17_0;

    /**
     * Reference to the minimum transport version that can be used with CCS.
     * This should be the transport version used by the previous minor release.
     */
    public static final TransportVersion MINIMUM_CCS_VERSION = V_8_12_0;

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

    static final IntFunction<String> VERSION_LOOKUP = ReleaseVersions.generateVersionsLookup(TransportVersions.class);

    // no instance
    private TransportVersions() {}
}
