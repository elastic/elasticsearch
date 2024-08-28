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
    public static final TransportVersion V_8_14_0 = def(8_636_00_1);
    // 8.15.0+
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
    public static final TransportVersion ML_INFERENCE_GOOGLE_VERTEX_AI_EMBEDDINGS_ADDED = def(8_694_00_0);
    public static final TransportVersion EVENT_INGESTED_RANGE_IN_CLUSTER_STATE = def(8_695_00_0);
    public static final TransportVersion ESQL_ADD_AGGREGATE_TYPE = def(8_696_00_0);
    public static final TransportVersion SECURITY_MIGRATIONS_MIGRATION_NEEDED_ADDED = def(8_697_00_0);
    public static final TransportVersion K_FOR_KNN_QUERY_ADDED = def(8_698_00_0);
    public static final TransportVersion TEXT_SIMILARITY_RERANKER_RETRIEVER = def(8_699_00_0);
    public static final TransportVersion ML_INFERENCE_GOOGLE_VERTEX_AI_RERANKING_ADDED = def(8_700_00_0);
    public static final TransportVersion VERSIONED_MASTER_NODE_REQUESTS = def(8_701_00_0);
    public static final TransportVersion ML_INFERENCE_AMAZON_BEDROCK_ADDED = def(8_702_00_0);
    public static final TransportVersion ENTERPRISE_GEOIP_DOWNLOADER_BACKPORT_8_15 = def(8_702_00_1);
    public static final TransportVersion FIX_VECTOR_SIMILARITY_INNER_HITS_BACKPORT_8_15 = def(8_702_00_2);
    public static final TransportVersion ML_INFERENCE_DONT_DELETE_WHEN_SEMANTIC_TEXT_EXISTS = def(8_703_00_0);
    public static final TransportVersion INFERENCE_ADAPTIVE_ALLOCATIONS = def(8_704_00_0);
    public static final TransportVersion INDEX_REQUEST_UPDATE_BY_SCRIPT_ORIGIN = def(8_705_00_0);
    public static final TransportVersion ML_INFERENCE_COHERE_UNUSED_RERANK_SETTINGS_REMOVED = def(8_706_00_0);
    public static final TransportVersion ENRICH_CACHE_STATS_SIZE_ADDED = def(8_707_00_0);
    public static final TransportVersion ENTERPRISE_GEOIP_DOWNLOADER = def(8_708_00_0);
    public static final TransportVersion NODES_STATS_ENUM_SET = def(8_709_00_0);
    public static final TransportVersion MASTER_NODE_METRICS = def(8_710_00_0);
    public static final TransportVersion SEGMENT_LEVEL_FIELDS_STATS = def(8_711_00_0);
    public static final TransportVersion ML_ADD_DETECTION_RULE_PARAMS = def(8_712_00_0);
    public static final TransportVersion FIX_VECTOR_SIMILARITY_INNER_HITS = def(8_713_00_0);
    public static final TransportVersion INDEX_REQUEST_UPDATE_BY_DOC_ORIGIN = def(8_714_00_0);
    public static final TransportVersion ESQL_ATTRIBUTE_CACHED_SERIALIZATION = def(8_715_00_0);
    public static final TransportVersion REGISTER_SLM_STATS = def(8_716_00_0);
    public static final TransportVersion ESQL_NESTED_UNSUPPORTED = def(8_717_00_0);
    public static final TransportVersion ESQL_SINGLE_VALUE_QUERY_SOURCE = def(8_718_00_0);
    public static final TransportVersion ESQL_ORIGINAL_INDICES = def(8_719_00_0);
    public static final TransportVersion ML_INFERENCE_EIS_INTEGRATION_ADDED = def(8_720_00_0);
    public static final TransportVersion INGEST_PIPELINE_EXCEPTION_ADDED = def(8_721_00_0);
    public static final TransportVersion ZDT_NANOS_SUPPORT_BROKEN = def(8_722_00_0);
    public static final TransportVersion REMOVE_GLOBAL_RETENTION_FROM_TEMPLATES = def(8_723_00_0);
    public static final TransportVersion RANDOM_RERANKER_RETRIEVER = def(8_724_00_0);
    public static final TransportVersion ESQL_PROFILE_SLEEPS = def(8_725_00_0);
    public static final TransportVersion ZDT_NANOS_SUPPORT = def(8_726_00_0);
    public static final TransportVersion LTR_SERVERLESS_RELEASE = def(8_727_00_0);
    public static final TransportVersion ALLOW_PARTIAL_SEARCH_RESULTS_IN_PIT = def(8_728_00_0);
    public static final TransportVersion RANK_DOCS_RETRIEVER = def(8_729_00_0);
    public static final TransportVersion ESQL_ES_FIELD_CACHED_SERIALIZATION = def(8_730_00_0);
    public static final TransportVersion ADD_MANAGE_ROLES_PRIVILEGE = def(8_731_00_0);
    public static final TransportVersion REPOSITORIES_TELEMETRY = def(8_732_00_0);
    public static final TransportVersion ML_INFERENCE_ALIBABACLOUD_SEARCH_ADDED = def(8_733_00_0);

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
    public static final TransportVersion MINIMUM_CCS_VERSION = FIX_VECTOR_SIMILARITY_INNER_HITS_BACKPORT_8_15;

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

    static final IntFunction<String> VERSION_LOOKUP = ReleaseVersions.generateVersionsLookup(TransportVersions.class, LATEST_DEFINED.id());

    // no instance
    private TransportVersions() {}
}
