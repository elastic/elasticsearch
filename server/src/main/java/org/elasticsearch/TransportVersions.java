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
    public static final TransportVersion V_8_12_1 = def(8_560_00_1);
    public static final TransportVersion V_8_13_0 = def(8_595_00_0);
    public static final TransportVersion V_8_13_4 = def(8_595_00_1);
    // 8.14.0+
    public static final TransportVersion RANDOM_AGG_SHARD_SEED = def(8_596_00_0);
    public static final TransportVersion ESQL_TIMINGS = def(8_597_00_0);
    public static final TransportVersion DATA_STREAM_AUTO_SHARDING_EVENT = def(8_598_00_0);
    public static final TransportVersion ADD_FAILURE_STORE_INDICES_OPTIONS = def(8_599_00_0);
    public static final TransportVersion ESQL_ENRICH_OPERATOR_STATUS = def(8_600_00_0);
    public static final TransportVersion ESQL_SERIALIZE_ARRAY_VECTOR = def(8_601_00_0);
    public static final TransportVersion ESQL_SERIALIZE_ARRAY_BLOCK = def(8_602_00_0);
    public static final TransportVersion ADD_DATA_STREAM_GLOBAL_RETENTION = def(8_603_00_0);
    public static final TransportVersion ALLOCATION_STATS = def(8_604_00_0);
    public static final TransportVersion ESQL_EXTENDED_ENRICH_TYPES = def(8_605_00_0);
    public static final TransportVersion KNN_EXPLICIT_BYTE_QUERY_VECTOR_PARSING = def(8_606_00_0);
    public static final TransportVersion ESQL_EXTENDED_ENRICH_INPUT_TYPE = def(8_607_00_0);
    public static final TransportVersion ESQL_SERIALIZE_BIG_VECTOR = def(8_608_00_0);
    public static final TransportVersion AGGS_EXCLUDED_DELETED_DOCS = def(8_609_00_0);
    public static final TransportVersion ESQL_SERIALIZE_BIG_ARRAY = def(8_610_00_0);
    public static final TransportVersion AUTO_SHARDING_ROLLOVER_CONDITION = def(8_611_00_0);
    public static final TransportVersion KNN_QUERY_VECTOR_BUILDER = def(8_612_00_0);
    public static final TransportVersion USE_DATA_STREAM_GLOBAL_RETENTION = def(8_613_00_0);
    public static final TransportVersion ML_COMPLETION_INFERENCE_SERVICE_ADDED = def(8_614_00_0);
    public static final TransportVersion ML_INFERENCE_EMBEDDING_BYTE_ADDED = def(8_615_00_0);
    public static final TransportVersion ML_INFERENCE_L2_NORM_SIMILARITY_ADDED = def(8_616_00_0);
    public static final TransportVersion SEARCH_NODE_LOAD_AUTOSCALING = def(8_617_00_0);
    public static final TransportVersion ESQL_ES_SOURCE_OPTIONS = def(8_618_00_0);
    public static final TransportVersion ADD_PERSISTENT_TASK_EXCEPTIONS = def(8_619_00_0);
    public static final TransportVersion ESQL_REDUCER_NODE_FRAGMENT = def(8_620_00_0);
    public static final TransportVersion FAILURE_STORE_ROLLOVER = def(8_621_00_0);
    public static final TransportVersion CCR_STATS_API_TIMEOUT_PARAM = def(8_622_00_0);
    public static final TransportVersion ESQL_ORDINAL_BLOCK = def(8_623_00_0);
    public static final TransportVersion ML_INFERENCE_COHERE_RERANK = def(8_624_00_0);
    public static final TransportVersion INDEXING_PRESSURE_DOCUMENT_REJECTIONS_COUNT = def(8_625_00_0);
    public static final TransportVersion ALIAS_ACTION_RESULTS = def(8_626_00_0);
    public static final TransportVersion HISTOGRAM_AGGS_KEY_SORTED = def(8_627_00_0);
    public static final TransportVersion INFERENCE_FIELDS_METADATA = def(8_628_00_0);
    public static final TransportVersion ML_INFERENCE_TIMEOUT_ADDED = def(8_629_00_0);
    public static final TransportVersion MODIFY_DATA_STREAM_FAILURE_STORES = def(8_630_00_0);
    public static final TransportVersion ML_INFERENCE_RERANK_NEW_RESPONSE_FORMAT = def(8_631_00_0);
    public static final TransportVersion HIGHLIGHTERS_TAGS_ON_FIELD_LEVEL = def(8_632_00_0);
    public static final TransportVersion TRACK_FLUSH_TIME_EXCLUDING_WAITING_ON_LOCKS = def(8_633_00_0);
    public static final TransportVersion ML_INFERENCE_AZURE_OPENAI_EMBEDDINGS = def(8_634_00_0);
    public static final TransportVersion ILM_SHRINK_ENABLE_WRITE = def(8_635_00_0);
    public static final TransportVersion GEOIP_CACHE_STATS = def(8_636_00_0);
    public static final TransportVersion SHUTDOWN_REQUEST_TIMEOUTS_FIX_8_14 = def(8_636_00_1);
    public static final TransportVersion WATERMARK_THRESHOLDS_STATS = def(8_637_00_0);
    public static final TransportVersion ENRICH_CACHE_ADDITIONAL_STATS = def(8_638_00_0);
    public static final TransportVersion ML_INFERENCE_RATE_LIMIT_SETTINGS_ADDED = def(8_639_00_0);
    public static final TransportVersion ML_TRAINED_MODEL_CACHE_METADATA_ADDED = def(8_640_00_0);
    public static final TransportVersion TOP_LEVEL_KNN_SUPPORT_QUERY_NAME = def(8_641_00_0);
    public static final TransportVersion INDEX_SEGMENTS_VECTOR_FORMATS = def(8_642_00_0);
    public static final TransportVersion ADD_RESOURCE_ALREADY_UPLOADED_EXCEPTION = def(8_643_00_0);
    public static final TransportVersion ESQL_MV_ORDERING_SORTED_ASCENDING = def(8_644_00_0);
    public static final TransportVersion ESQL_PAGE_MAPPING_TO_ITERATOR = def(8_645_00_0);
    public static final TransportVersion BINARY_PIT_ID = def(8_646_00_0);
    public static final TransportVersion SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE = def(8_647_00_0);
    public static final TransportVersion ESQL_REQUEST_TABLES = def(8_648_00_0);
    public static final TransportVersion ROLE_REMOTE_CLUSTER_PRIVS = def(8_649_00_0);
    public static final TransportVersion NO_GLOBAL_RETENTION_FOR_SYSTEM_DATA_STREAMS = def(8_650_00_0);
    public static final TransportVersion SHUTDOWN_REQUEST_TIMEOUTS_FIX = def(8_651_00_0);
    public static final TransportVersion INDEXING_PRESSURE_REQUEST_REJECTIONS_COUNT = def(8_652_00_0);
    public static final TransportVersion ROLLUP_USAGE = def(8_653_00_0);
    public static final TransportVersion SECURITY_ROLE_DESCRIPTION = def(8_654_00_0);
    public static final TransportVersion ML_INFERENCE_AZURE_OPENAI_COMPLETIONS = def(8_655_00_0);
    public static final TransportVersion JOIN_STATUS_AGE_SERIALIZATION = def(8_656_00_0);
    public static final TransportVersion ML_RERANK_DOC_OPTIONAL = def(8_657_00_0);
    public static final TransportVersion FAILURE_STORE_FIELD_PARITY = def(8_658_00_0);
    public static final TransportVersion ML_INFERENCE_AZURE_AI_STUDIO = def(8_659_00_0);
    public static final TransportVersion ML_INFERENCE_COHERE_COMPLETION_ADDED = def(8_660_00_0);
    public static final TransportVersion ESQL_REMOVE_ES_SOURCE_OPTIONS = def(8_661_00_0);
    public static final TransportVersion NODE_STATS_INGEST_BYTES = def(8_662_00_0);
    public static final TransportVersion SEMANTIC_QUERY = def(8_663_00_0);
    public static final TransportVersion GET_AUTOSCALING_CAPACITY_UNUSED_TIMEOUT = def(8_664_00_0);
    public static final TransportVersion SIMULATE_VALIDATES_MAPPINGS = def(8_665_00_0);
    public static final TransportVersion RULE_QUERY_RENAME = def(8_666_00_0);
    public static final TransportVersion SPARSE_VECTOR_QUERY_ADDED = def(8_667_00_0);
    public static final TransportVersion ESQL_ADD_INDEX_MODE_TO_SOURCE = def(8_668_00_0);
    public static final TransportVersion GET_SHUTDOWN_STATUS_TIMEOUT = def(8_669_00_0);
    public static final TransportVersion FAILURE_STORE_TELEMETRY = def(8_670_00_0);
    public static final TransportVersion ADD_METADATA_FLATTENED_TO_ROLES = def(8_671_00_0);
    public static final TransportVersion ML_INFERENCE_GOOGLE_AI_STUDIO_COMPLETION_ADDED = def(8_672_00_0);
    public static final TransportVersion WATCHER_REQUEST_TIMEOUTS = def(8_673_00_0);
    public static final TransportVersion ML_INFERENCE_ENHANCE_DELETE_ENDPOINT = def(8_674_00_0);
    public static final TransportVersion ML_INFERENCE_GOOGLE_AI_STUDIO_EMBEDDINGS_ADDED = def(8_675_00_0);
    public static final TransportVersion ADD_MISTRAL_EMBEDDINGS_INFERENCE = def(8_676_00_0);
    public static final TransportVersion ML_CHUNK_INFERENCE_OPTION = def(8_677_00_0);
    public static final TransportVersion RANK_FEATURE_PHASE_ADDED = def(8_678_00_0);
    public static final TransportVersion RANK_DOC_IN_SHARD_FETCH_REQUEST = def(8_679_00_0);
    public static final TransportVersion SECURITY_SETTINGS_REQUEST_TIMEOUTS = def(8_680_00_0);
    public static final TransportVersion QUERY_RULE_CRUD_API_PUT = def(8_681_00_0);
    public static final TransportVersion DROP_UNUSED_NODES_REQUESTS = def(8_682_00_0);
    public static final TransportVersion QUERY_RULE_CRUD_API_GET_DELETE = def(8_683_00_0);
    public static final TransportVersion MORE_LIGHTER_NODES_REQUESTS = def(8_684_00_0);
    public static final TransportVersion DROP_UNUSED_NODES_IDS = def(8_685_00_0);
    public static final TransportVersion DELETE_SNAPSHOTS_ASYNC_ADDED = def(8_686_00_0);
    public static final TransportVersion VERSION_SUPPORTING_SPARSE_VECTOR_STATS = def(8_687_00_0);
    public static final TransportVersion ML_AD_OUTPUT_MEMORY_ALLOCATOR_FIELD = def(8_688_00_0);
    public static final TransportVersion FAILURE_STORE_LAZY_CREATION = def(8_689_00_0);
    public static final TransportVersion SNAPSHOT_REQUEST_TIMEOUTS = def(8_690_00_0);
    public static final TransportVersion INDEX_METADATA_MAPPINGS_UPDATED_VERSION = def(8_691_00_0);
    public static final TransportVersion ML_INFERENCE_ELAND_SETTINGS_ADDED = def(8_692_00_0);
    public static final TransportVersion ML_ANTHROPIC_INTEGRATION_ADDED = def(8_693_00_0);

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
    public static final TransportVersion MINIMUM_CCS_VERSION = SHUTDOWN_REQUEST_TIMEOUTS_FIX_8_14;

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
