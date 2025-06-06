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
    public static final TransportVersion V_8_9_X = def(8_500_0_20);
    public static final TransportVersion V_8_10_X = def(8_500_0_61);
    public static final TransportVersion V_8_11_X = def(8_512_0_01);
    public static final TransportVersion V_8_12_0 = def(8_560_0_00);
    public static final TransportVersion V_8_12_1 = def(8_560_0_01);
    public static final TransportVersion V_8_13_0 = def(8_595_0_00);
    public static final TransportVersion V_8_13_4 = def(8_595_0_01);
    public static final TransportVersion V_8_14_0 = def(8_636_0_01);
    public static final TransportVersion V_8_15_0 = def(8_702_0_02);
    public static final TransportVersion V_8_15_2 = def(8_702_0_03);
    public static final TransportVersion V_8_16_0 = def(8_772_0_01);
    public static final TransportVersion ADD_COMPATIBILITY_VERSIONS_TO_NODE_INFO_BACKPORT_8_16 = def(8_772_0_02);
    public static final TransportVersion SKIP_INNER_HITS_SEARCH_SOURCE_BACKPORT_8_16 = def(8_772_0_03);
    public static final TransportVersion QUERY_RULES_LIST_INCLUDES_TYPES_BACKPORT_8_16 = def(8_772_0_04);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_16_5 = def(8_772_0_05);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_16_6 = def(8_772_0_06);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_16_7 = def(8_772_0_07);
    public static final TransportVersion REMOVE_MIN_COMPATIBLE_SHARD_NODE = def(8_773_0_00);
    public static final TransportVersion REVERT_REMOVE_MIN_COMPATIBLE_SHARD_NODE = def(8_774_0_00);
    public static final TransportVersion ESQL_FIELD_ATTRIBUTE_PARENT_SIMPLIFIED = def(8_775_0_00);
    public static final TransportVersion INFERENCE_DONT_PERSIST_ON_READ = def(8_776_0_00);
    public static final TransportVersion SIMULATE_MAPPING_ADDITION = def(8_777_0_00);
    public static final TransportVersion INTRODUCE_ALL_APPLICABLE_SELECTOR = def(8_778_0_00);
    public static final TransportVersion INDEX_MODE_LOOKUP = def(8_779_0_00);
    public static final TransportVersion INDEX_REQUEST_REMOVE_METERING = def(8_780_0_00);
    public static final TransportVersion CPU_STAT_STRING_PARSING = def(8_781_0_00);
    public static final TransportVersion QUERY_RULES_RETRIEVER = def(8_782_0_00);
    public static final TransportVersion ESQL_CCS_EXEC_INFO_WITH_FAILURES = def(8_783_0_00);
    public static final TransportVersion LOGSDB_TELEMETRY = def(8_784_0_00);
    public static final TransportVersion LOGSDB_TELEMETRY_STATS = def(8_785_0_00);
    public static final TransportVersion KQL_QUERY_ADDED = def(8_786_0_00);
    public static final TransportVersion ROLE_MONITOR_STATS = def(8_787_0_00);
    public static final TransportVersion DATA_STREAM_INDEX_VERSION_DEPRECATION_CHECK = def(8_788_0_00);
    public static final TransportVersion ADD_COMPATIBILITY_VERSIONS_TO_NODE_INFO = def(8_789_0_00);
    public static final TransportVersion VERTEX_AI_INPUT_TYPE_ADDED = def(8_790_0_00);
    public static final TransportVersion SKIP_INNER_HITS_SEARCH_SOURCE = def(8_791_0_00);
    public static final TransportVersion QUERY_RULES_LIST_INCLUDES_TYPES = def(8_792_0_00);
    public static final TransportVersion INDEX_STATS_ADDITIONAL_FIELDS = def(8_793_0_00);
    public static final TransportVersion INDEX_STATS_ADDITIONAL_FIELDS_REVERT = def(8_794_0_00);
    public static final TransportVersion FAST_REFRESH_RCO_2 = def(8_795_0_00);
    public static final TransportVersion ESQL_ENRICH_RUNTIME_WARNINGS = def(8_796_0_00);
    public static final TransportVersion INGEST_PIPELINE_CONFIGURATION_AS_MAP = def(8_797_0_00);
    public static final TransportVersion LOGSDB_TELEMETRY_CUSTOM_CUTOFF_DATE_FIX_8_17 = def(8_797_0_01);
    public static final TransportVersion SOURCE_MODE_TELEMETRY_FIX_8_17 = def(8_797_0_02);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_17_3 = def(8_797_0_03);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_17_4 = def(8_797_0_04);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_17_5 = def(8_797_0_05);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_17_6 = def(8_797_0_06);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_17_7 = def(8_797_0_07);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_17_8 = def(8_797_0_08);
    public static final TransportVersion INDEXING_PRESSURE_THROTTLING_STATS = def(8_798_0_00);
    public static final TransportVersion REINDEX_DATA_STREAMS = def(8_799_0_00);
    public static final TransportVersion ESQL_REMOVE_NODE_LEVEL_PLAN = def(8_800_0_00);
    public static final TransportVersion LOGSDB_TELEMETRY_CUSTOM_CUTOFF_DATE = def(8_801_0_00);
    public static final TransportVersion SOURCE_MODE_TELEMETRY = def(8_802_0_00);
    public static final TransportVersion NEW_REFRESH_CLUSTER_BLOCK = def(8_803_0_00);
    public static final TransportVersion RETRIES_AND_OPERATIONS_IN_BLOBSTORE_STATS = def(8_804_0_00);
    public static final TransportVersion ADD_DATA_STREAM_OPTIONS_TO_TEMPLATES = def(8_805_0_00);
    public static final TransportVersion KNN_QUERY_RESCORE_OVERSAMPLE = def(8_806_0_00);
    public static final TransportVersion SEMANTIC_QUERY_LENIENT = def(8_807_0_00);
    public static final TransportVersion ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS = def(8_808_0_00);
    public static final TransportVersion EQL_ALLOW_PARTIAL_SEARCH_RESULTS = def(8_809_0_00);
    public static final TransportVersion NODE_VERSION_INFORMATION_WITH_MIN_READ_ONLY_INDEX_VERSION = def(8_810_0_00);
    public static final TransportVersion ERROR_TRACE_IN_TRANSPORT_HEADER = def(8_811_0_00);
    public static final TransportVersion FAILURE_STORE_ENABLED_BY_CLUSTER_SETTING = def(8_812_0_00);
    public static final TransportVersion SIMULATE_IGNORED_FIELDS = def(8_813_0_00);
    public static final TransportVersion TRANSFORMS_UPGRADE_MODE = def(8_814_0_00);
    public static final TransportVersion NODE_SHUTDOWN_EPHEMERAL_ID_ADDED = def(8_815_0_00);
    public static final TransportVersion ESQL_CCS_TELEMETRY_STATS = def(8_816_0_00);
    public static final TransportVersion TEXT_EMBEDDING_QUERY_VECTOR_BUILDER_INFER_MODEL_ID = def(8_817_0_00);
    public static final TransportVersion ESQL_ENABLE_NODE_LEVEL_REDUCTION = def(8_818_0_00);
    public static final TransportVersion JINA_AI_INTEGRATION_ADDED = def(8_819_0_00);
    public static final TransportVersion TRACK_INDEX_FAILED_DUE_TO_VERSION_CONFLICT_METRIC = def(8_820_0_00);
    public static final TransportVersion REPLACE_FAILURE_STORE_OPTIONS_WITH_SELECTOR_SYNTAX = def(8_821_0_00);
    public static final TransportVersion ELASTIC_INFERENCE_SERVICE_UNIFIED_CHAT_COMPLETIONS_INTEGRATION = def(8_822_0_00);
    public static final TransportVersion KQL_QUERY_TECH_PREVIEW = def(8_823_0_00);
    public static final TransportVersion ESQL_PROFILE_ROWS_PROCESSED = def(8_824_0_00);
    public static final TransportVersion BYTE_SIZE_VALUE_ALWAYS_USES_BYTES_1 = def(8_825_0_00);
    public static final TransportVersion REVERT_BYTE_SIZE_VALUE_ALWAYS_USES_BYTES_1 = def(8_826_0_00);
    public static final TransportVersion ESQL_SKIP_ES_INDEX_SERIALIZATION = def(8_827_0_00);
    public static final TransportVersion ADD_INDEX_BLOCK_TWO_PHASE = def(8_828_0_00);
    public static final TransportVersion RESOLVE_CLUSTER_NO_INDEX_EXPRESSION = def(8_829_0_00);
    public static final TransportVersion ML_ROLLOVER_LEGACY_INDICES = def(8_830_0_00);
    public static final TransportVersion ADD_INCLUDE_FAILURE_INDICES_OPTION = def(8_831_0_00);
    public static final TransportVersion ESQL_RESPONSE_PARTIAL = def(8_832_0_00);
    public static final TransportVersion RANK_DOC_OPTIONAL_METADATA_FOR_EXPLAIN = def(8_833_0_00);
    public static final TransportVersion ILM_ADD_SEARCHABLE_SNAPSHOT_ADD_REPLICATE_FOR = def(8_834_0_00);
    public static final TransportVersion INGEST_REQUEST_INCLUDE_SOURCE_ON_ERROR = def(8_835_0_00);
    public static final TransportVersion RESOURCE_DEPRECATION_CHECKS = def(8_836_0_00);
    public static final TransportVersion LINEAR_RETRIEVER_SUPPORT = def(8_837_0_00);
    public static final TransportVersion TIMEOUT_GET_PARAM_FOR_RESOLVE_CLUSTER = def(8_838_0_00);
    public static final TransportVersion INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING = def(8_839_0_00);
    public static final TransportVersion ML_INFERENCE_IBM_WATSONX_RERANK_ADDED = def(8_840_0_00);
    public static final TransportVersion REMOVE_ALL_APPLICABLE_SELECTOR_BACKPORT_8_18 = def(8_840_0_01);
    public static final TransportVersion RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR_8_18 = def(8_840_0_02);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_18_1 = def(8_840_0_03);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_18_2 = def(8_840_0_04);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_18_3 = def(8_840_0_05);
    public static final TransportVersion INITIAL_ELASTICSEARCH_8_19 = def(8_841_0_00);
    public static final TransportVersion COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_X = def(8_841_0_01);
    public static final TransportVersion REMOVE_ALL_APPLICABLE_SELECTOR_BACKPORT_8_19 = def(8_841_0_02);
    public static final TransportVersion ESQL_RETRY_ON_SHARD_LEVEL_FAILURE_BACKPORT_8_19 = def(8_841_0_03);
    public static final TransportVersion ESQL_SUPPORT_PARTIAL_RESULTS_BACKPORT_8_19 = def(8_841_0_04);
    public static final TransportVersion VOYAGE_AI_INTEGRATION_ADDED_BACKPORT_8_X = def(8_841_0_05);
    public static final TransportVersion JINA_AI_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_19 = def(8_841_0_06);
    public static final TransportVersion RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR_8_19 = def(8_841_0_07);
    public static final TransportVersion INFERENCE_CONTEXT_8_X = def(8_841_0_08);
    public static final TransportVersion ML_INFERENCE_DEEPSEEK_8_19 = def(8_841_0_09);
    public static final TransportVersion ESQL_SERIALIZE_BLOCK_TYPE_CODE = def(8_841_0_10);
    public static final TransportVersion ESQL_FAILURE_FROM_REMOTE = def(8_841_0_11);
    public static final TransportVersion ESQL_AGGREGATE_METRIC_DOUBLE_LITERAL = def(8_841_0_12);
    public static final TransportVersion INFERENCE_MODEL_REGISTRY_METADATA_8_19 = def(8_841_0_13);
    public static final TransportVersion INTRODUCE_LIFECYCLE_TEMPLATE_8_19 = def(8_841_0_14);
    public static final TransportVersion RERANK_COMMON_OPTIONS_ADDED_8_19 = def(8_841_0_15);
    public static final TransportVersion REMOTE_EXCEPTION_8_19 = def(8_841_0_16);
    public static final TransportVersion AMAZON_BEDROCK_TASK_SETTINGS_8_19 = def(8_841_0_17);
    public static final TransportVersion SEMANTIC_TEXT_CHUNKING_CONFIG_8_19 = def(8_841_0_18);
    public static final TransportVersion BATCHED_QUERY_PHASE_VERSION_BACKPORT_8_X = def(8_841_0_19);
    public static final TransportVersion SEARCH_INCREMENTAL_TOP_DOCS_NULL_BACKPORT_8_19 = def(8_841_0_20);
    public static final TransportVersion ML_INFERENCE_SAGEMAKER_8_19 = def(8_841_0_21);
    public static final TransportVersion ESQL_REPORT_ORIGINAL_TYPES_BACKPORT_8_19 = def(8_841_0_22);
    public static final TransportVersion PINNED_RETRIEVER_8_19 = def(8_841_0_23);
    public static final TransportVersion ESQL_AGGREGATE_METRIC_DOUBLE_BLOCK_8_19 = def(8_841_0_24);
    public static final TransportVersion INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19 = def(8_841_0_25);
    public static final TransportVersion INTRODUCE_FAILURES_DEFAULT_RETENTION_BACKPORT_8_19 = def(8_841_0_26);
    public static final TransportVersion RESCORE_VECTOR_ALLOW_ZERO_BACKPORT_8_19 = def(8_841_0_27);
    public static final TransportVersion INFERENCE_ADD_TIMEOUT_PUT_ENDPOINT_8_19 = def(8_841_0_28);
    public static final TransportVersion ESQL_REPORT_SHARD_PARTITIONING_8_19 = def(8_841_0_29);
    public static final TransportVersion ESQL_DRIVER_TASK_DESCRIPTION_8_19 = def(8_841_0_30);
    public static final TransportVersion ML_INFERENCE_HUGGING_FACE_CHAT_COMPLETION_ADDED_8_19 = def(8_841_0_31);
    public static final TransportVersion FIELD_CAPS_ADD_CLUSTER_ALIAS = def(8_841_0_32);
    public static final TransportVersion INCLUDE_INDEX_MODE_IN_GET_DATA_STREAM_BACKPORT_8_19 = def(8_841_0_33);
    public static final TransportVersion ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME_8_19 = def(8_841_0_34);
    public static final TransportVersion RERANKER_FAILURES_ALLOWED_8_19 = def(8_841_0_35);
    public static final TransportVersion ML_INFERENCE_HUGGING_FACE_RERANK_ADDED_8_19 = def(8_841_0_36);
    public static final TransportVersion ML_INFERENCE_SAGEMAKER_CHAT_COMPLETION_8_19 = def(8_841_0_37);
    public static final TransportVersion ML_INFERENCE_VERTEXAI_CHATCOMPLETION_ADDED_8_19 = def(8_841_0_38);
    public static final TransportVersion INFERENCE_CUSTOM_SERVICE_ADDED_8_19 = def(8_841_0_39);
    public static final TransportVersion IDP_CUSTOM_SAML_ATTRIBUTES_ADDED_8_19 = def(8_841_0_40);
    public static final TransportVersion DATA_STREAM_OPTIONS_API_REMOVE_INCLUDE_DEFAULTS_8_19 = def(8_841_0_41);
    public static final TransportVersion JOIN_ON_ALIASES_8_19 = def(8_841_0_42);
    public static final TransportVersion ILM_ADD_SKIP_SETTING_8_19 = def(8_841_0_43);
    public static final TransportVersion ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY_8_19 = def(8_841_0_44);
    public static final TransportVersion ML_INFERENCE_MISTRAL_CHAT_COMPLETION_ADDED_8_19 = def(8_841_0_45);
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
     * M_NNN_S_PP
     *
     * M - The major version of Elasticsearch
     * NNN - The server version part
     * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
     * PP - The patch version part
     *
     * To determine the id of the next TransportVersion constant, do the following:
     * - Use the same major version, unless bumping majors
     * - Bump the server version part by 1, unless creating a patch version
     * - Leave the subsidiary part as 0
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
    public static final TransportVersion MINIMUM_CCS_VERSION = RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR_8_18;

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
