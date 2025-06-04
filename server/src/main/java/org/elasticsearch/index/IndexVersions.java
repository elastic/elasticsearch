/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.ReleaseVersions;
import org.elasticsearch.core.Assertions;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntFunction;

@SuppressWarnings("deprecation")
public class IndexVersions {

    /*
     * NOTE: IntelliJ lies!
     * This map is used during class construction, referenced by the registerIndexVersion method.
     * When all the index version constants have been registered, the map is cleared & never touched again.
     */
    @SuppressWarnings("UnusedAssignment")
    static TreeSet<Integer> IDS = new TreeSet<>();

    private static IndexVersion def(int id, Version luceneVersion) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        if (IDS.add(id) == false) {
            throw new IllegalArgumentException("Version id " + id + " defined twice");
        }
        if (id < IDS.last()) {
            throw new IllegalArgumentException("Version id " + id + " is not defined in the right location. Keep constants sorted");
        }
        return new IndexVersion(id, luceneVersion);
    }

    // TODO: this is just a hack to allow to keep the V7 IndexVersion constants, during compilation. Remove
    private static Version parseUnchecked(String version) {
        try {
            return Version.parse(version);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static final IndexVersion ZERO = def(0, Version.LATEST);

    public static final IndexVersion V_7_0_0 = def(7_00_00_99, parseUnchecked("8.0.0"));
    public static final IndexVersion V_7_1_0 = def(7_01_00_99, parseUnchecked("8.0.0"));
    public static final IndexVersion V_7_2_0 = def(7_02_00_99, parseUnchecked("8.0.0"));
    public static final IndexVersion V_7_2_1 = def(7_02_01_99, parseUnchecked("8.0.0"));
    public static final IndexVersion V_7_3_0 = def(7_03_00_99, parseUnchecked("8.1.0"));
    public static final IndexVersion V_7_4_0 = def(7_04_00_99, parseUnchecked("8.2.0"));
    public static final IndexVersion V_7_5_0 = def(7_05_00_99, parseUnchecked("8.3.0"));
    public static final IndexVersion V_7_5_2 = def(7_05_02_99, parseUnchecked("8.3.0"));
    public static final IndexVersion V_7_6_0 = def(7_06_00_99, parseUnchecked("8.4.0"));
    public static final IndexVersion V_7_7_0 = def(7_07_00_99, parseUnchecked("8.5.1"));
    public static final IndexVersion V_7_8_0 = def(7_08_00_99, parseUnchecked("8.5.1"));
    public static final IndexVersion V_7_9_0 = def(7_09_00_99, parseUnchecked("8.6.0"));
    public static final IndexVersion V_7_10_0 = def(7_10_00_99, parseUnchecked("8.7.0"));
    public static final IndexVersion V_7_11_0 = def(7_11_00_99, parseUnchecked("8.7.0"));
    public static final IndexVersion V_7_12_0 = def(7_12_00_99, parseUnchecked("8.8.0"));
    public static final IndexVersion V_7_13_0 = def(7_13_00_99, parseUnchecked("8.8.2"));
    public static final IndexVersion V_7_14_0 = def(7_14_00_99, parseUnchecked("8.9.0"));
    public static final IndexVersion V_7_15_0 = def(7_15_00_99, parseUnchecked("8.9.0"));
    public static final IndexVersion V_7_16_0 = def(7_16_00_99, parseUnchecked("8.10.1"));
    public static final IndexVersion V_7_17_0 = def(7_17_00_99, parseUnchecked("8.11.1"));
    public static final IndexVersion V_8_0_0 = def(8_00_00_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_1_0 = def(8_01_00_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_2_0 = def(8_02_00_99, Version.LUCENE_9_1_0);
    public static final IndexVersion V_8_3_0 = def(8_03_00_99, Version.LUCENE_9_2_0);
    public static final IndexVersion V_8_4_0 = def(8_04_00_99, Version.LUCENE_9_3_0);
    public static final IndexVersion V_8_5_0 = def(8_05_00_99, Version.LUCENE_9_4_1);
    public static final IndexVersion V_8_5_3 = def(8_05_03_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_6_0 = def(8_06_00_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_7_0 = def(8_07_00_99, Version.LUCENE_9_5_0);
    public static final IndexVersion V_8_8_0 = def(8_08_00_99, Version.LUCENE_9_6_0);
    public static final IndexVersion V_8_8_2 = def(8_08_02_99, Version.LUCENE_9_6_0);
    public static final IndexVersion V_8_9_0 = def(8_09_00_99, Version.LUCENE_9_7_0);
    public static final IndexVersion V_8_9_1 = def(8_09_01_99, Version.LUCENE_9_7_0);
    public static final IndexVersion V_8_10_0 = def(8_10_00_99, Version.LUCENE_9_7_0);
    /*
     * READ THE COMMENT BELOW THIS BLOCK OF DECLARATIONS BEFORE ADDING NEW INDEX VERSIONS
     * Detached index versions added below here.
     */
    public static final IndexVersion FIRST_DETACHED_INDEX_VERSION = def(8_500_000, Version.LUCENE_9_7_0);
    public static final IndexVersion NEW_SPARSE_VECTOR = def(8_500_001, Version.LUCENE_9_7_0);
    public static final IndexVersion SPARSE_VECTOR_IN_FIELD_NAMES_SUPPORT = def(8_500_002, Version.LUCENE_9_7_0);
    public static final IndexVersion UPGRADE_LUCENE_9_8 = def(8_500_003, Version.LUCENE_9_8_0);
    public static final IndexVersion ES_VERSION_8_12 = def(8_500_004, Version.LUCENE_9_8_0);
    public static final IndexVersion NORMALIZED_VECTOR_COSINE = def(8_500_005, Version.LUCENE_9_8_0);
    public static final IndexVersion UPGRADE_LUCENE_9_9 = def(8_500_006, Version.LUCENE_9_9_0);
    public static final IndexVersion NORI_DUPLICATES = def(8_500_007, Version.LUCENE_9_9_0);
    public static final IndexVersion UPGRADE_LUCENE_9_9_1 = def(8_500_008, Version.LUCENE_9_9_1);
    public static final IndexVersion ES_VERSION_8_12_1 = def(8_500_009, Version.LUCENE_9_9_1);
    public static final IndexVersion UPGRADE_8_12_1_LUCENE_9_9_2 = def(8_500_010, Version.LUCENE_9_9_2);
    public static final IndexVersion NEW_INDEXVERSION_FORMAT = def(8_501_0_00, Version.LUCENE_9_9_1);
    public static final IndexVersion UPGRADE_LUCENE_9_9_2 = def(8_502_0_00, Version.LUCENE_9_9_2);
    public static final IndexVersion TIME_SERIES_ID_HASHING = def(8_502_0_01, Version.LUCENE_9_9_2);
    public static final IndexVersion UPGRADE_TO_LUCENE_9_10 = def(8_503_0_00, Version.LUCENE_9_10_0);
    public static final IndexVersion TIME_SERIES_ROUTING_HASH_IN_ID = def(8_504_0_00, Version.LUCENE_9_10_0);
    public static final IndexVersion DEFAULT_DENSE_VECTOR_TO_INT8_HNSW = def(8_505_0_00, Version.LUCENE_9_10_0);
    public static final IndexVersion DOC_VALUES_FOR_IGNORED_META_FIELD = def(8_505_0_01, Version.LUCENE_9_10_0);
    public static final IndexVersion SOURCE_MAPPER_LOSSY_PARAMS_CHECK = def(8_506_0_00, Version.LUCENE_9_10_0);
    public static final IndexVersion SEMANTIC_TEXT_FIELD_TYPE = def(8_507_0_00, Version.LUCENE_9_10_0);
    public static final IndexVersion UPGRADE_TO_LUCENE_9_11 = def(8_508_0_00, Version.LUCENE_9_11_0);
    public static final IndexVersion UNIQUE_TOKEN_FILTER_POS_FIX = def(8_509_0_00, Version.LUCENE_9_11_0);
    public static final IndexVersion ADD_SECURITY_MIGRATION = def(8_510_0_00, Version.LUCENE_9_11_0);
    public static final IndexVersion UPGRADE_TO_LUCENE_9_11_1 = def(8_511_0_00, Version.LUCENE_9_11_1);
    public static final IndexVersion INDEX_SORTING_ON_NESTED = def(8_512_0_00, Version.LUCENE_9_11_1);
    public static final IndexVersion LENIENT_UPDATEABLE_SYNONYMS = def(8_513_0_00, Version.LUCENE_9_11_1);
    public static final IndexVersion ENABLE_IGNORE_MALFORMED_LOGSDB = def(8_514_0_00, Version.LUCENE_9_11_1);
    public static final IndexVersion MERGE_ON_RECOVERY_VERSION = def(8_515_0_00, Version.LUCENE_9_11_1);
    public static final IndexVersion UPGRADE_TO_LUCENE_9_12 = def(8_516_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion ENABLE_IGNORE_ABOVE_LOGSDB = def(8_517_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion ADD_ROLE_MAPPING_CLEANUP_MIGRATION = def(8_518_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT_BACKPORT = def(8_519_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion TIME_BASED_K_ORDERED_DOC_ID_BACKPORT = def(8_520_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion V8_DEPRECATE_SOURCE_MODE_MAPPER = def(8_521_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion USE_SYNTHETIC_SOURCE_FOR_RECOVERY_BACKPORT = def(8_522_0_00, Version.LUCENE_9_12_0);
    public static final IndexVersion UPGRADE_TO_LUCENE_9_12_1 = def(8_523_0_00, parseUnchecked("9.12.1"));
    public static final IndexVersion INFERENCE_METADATA_FIELDS_BACKPORT = def(8_524_0_00, parseUnchecked("9.12.1"));
    public static final IndexVersion LOGSB_OPTIONAL_SORTING_ON_HOST_NAME_BACKPORT = def(8_525_0_00, parseUnchecked("9.12.1"));
    public static final IndexVersion USE_SYNTHETIC_SOURCE_FOR_RECOVERY_BY_DEFAULT_BACKPORT = def(8_526_0_00, parseUnchecked("9.12.1"));
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_BACKPORT_8_X = def(8_527_0_00, Version.LUCENE_9_12_1);
    public static final IndexVersion ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS_BACKPORT_8_X = def(8_528_0_00, Version.LUCENE_9_12_1);
    public static final IndexVersion RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS_BACKPORT_8_X = def(8_529_0_00, Version.LUCENE_9_12_1);
    public static final IndexVersion DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ_BACKPORT_8_X = def(8_530_0_00, Version.LUCENE_9_12_1);
    public static final IndexVersion SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X = def(8_531_0_00, Version.LUCENE_9_12_1);
    public static final IndexVersion UPGRADE_TO_LUCENE_10_0_0 = def(9_000_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT = def(9_001_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion TIME_BASED_K_ORDERED_DOC_ID = def(9_002_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion DEPRECATE_SOURCE_MODE_MAPPER = def(9_003_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion USE_SYNTHETIC_SOURCE_FOR_RECOVERY = def(9_004_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion INFERENCE_METADATA_FIELDS = def(9_005_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion LOGSB_OPTIONAL_SORTING_ON_HOST_NAME = def(9_006_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion SOURCE_MAPPER_MODE_ATTRIBUTE_NOOP = def(9_007_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion HOSTNAME_DOC_VALUES_SPARSE_INDEX = def(9_008_0_00, Version.LUCENE_10_0_0);
    public static final IndexVersion UPGRADE_TO_LUCENE_10_1_0 = def(9_009_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion USE_SYNTHETIC_SOURCE_FOR_RECOVERY_BY_DEFAULT = def(9_010_00_0, Version.LUCENE_10_1_0);
    public static final IndexVersion TIMESTAMP_DOC_VALUES_SPARSE_INDEX = def(9_011_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion TIME_SERIES_ID_DOC_VALUES_SPARSE_INDEX = def(9_012_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_KEYWORD = def(9_013_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_IP = def(9_014_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion ADD_RESCORE_PARAMS_TO_QUANTIZED_VECTORS = def(9_015_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_NUMBER = def(9_016_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_BOOLEAN = def(9_017_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion RESCORE_PARAMS_ALLOW_ZERO_TO_QUANTIZED_VECTORS = def(9_018_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_UNSIGNED_LONG = def(9_019_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_SCALED_FLOAT = def(9_020_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion USE_LUCENE101_POSTINGS_FORMAT = def(9_021_0_00, Version.LUCENE_10_1_0);
    public static final IndexVersion UPGRADE_TO_LUCENE_10_2_0 = def(9_022_00_0, Version.LUCENE_10_2_0);
    public static final IndexVersion UPGRADE_TO_LUCENE_10_2_1 = def(9_023_00_0, Version.LUCENE_10_2_1);
    public static final IndexVersion DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ = def(9_024_0_00, Version.LUCENE_10_2_1);
    public static final IndexVersion SEMANTIC_TEXT_DEFAULTS_TO_BBQ = def(9_025_0_00, Version.LUCENE_10_2_1);
    public static final IndexVersion DEFAULT_TO_ACORN_HNSW_FILTER_HEURISTIC = def(9_026_0_00, Version.LUCENE_10_2_1);
    public static final IndexVersion SEQ_NO_WITHOUT_POINTS = def(9_027_0_00, Version.LUCENE_10_2_1);
    public static final IndexVersion INDEX_INT_SORT_INT_TYPE = def(9_028_0_00, Version.LUCENE_10_2_1);

    public static final IndexVersion UPGRADE_TO_LUCENE_10_3_0 = def(9_050_00_0, Version.LUCENE_10_3_0);

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
     * ADDING AN INDEX VERSION
     * To add a new index version, add a new constant at the bottom of the list, above this comment. Don't add other lines,
     * comments, etc. The version id has the following layout:
     *
     * M_NNN_S_PP
     *
     * M - The major version of Elasticsearch
     * NNN - The server version part
     * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
     * PP - The patch version part
     *
     * To determine the id of the next IndexVersion constant, do the following:
     * - Use the same major version, unless bumping majors
     * - Bump the server version part by 1, unless creating a patch version
     * - Leave the subsidiary part as 0
     * - Bump the patch part if creating a patch version
     *
     * If a patch version is created, it should be placed sorted among the other existing constants.
     *
     * REVERTING AN INDEX VERSION
     *
     * If you revert a commit with an index version change, you MUST ensure there is a NEW index version representing the reverted
     * change. DO NOT let the index version go backwards, it must ALWAYS be incremented.
     *
     * DETERMINING INDEX VERSIONS FROM GIT HISTORY
     *
     * If your git checkout has the expected minor-version-numbered branches and the expected release-version tags then you can find the
     * index versions known by a particular release ...
     *
     *     git show v8.12.0:server/src/main/java/org/elasticsearch/index/IndexVersions.java | grep '= def'
     *
     * ... or by a particular branch ...
     *
     *     git show 8.12:server/src/main/java/org/elasticsearch/index/IndexVersions.java | grep '= def'
     *
     * ... and you can see which versions were added in between two versions too ...
     *
     *     git diff v8.12.0..main -- server/src/main/java/org/elasticsearch/index/IndexVersions.java
     *
     * In branches 8.7-8.11 see server/src/main/java/org/elasticsearch/index/IndexVersion.java for the equivalent definitions.
     */

    public static final IndexVersion MINIMUM_COMPATIBLE = V_8_0_0;
    public static final IndexVersion MINIMUM_READONLY_COMPATIBLE = V_7_0_0;

    static final NavigableMap<Integer, IndexVersion> VERSION_IDS = getAllVersionIds(IndexVersions.class);
    static final IndexVersion LATEST_DEFINED;
    static {
        LATEST_DEFINED = VERSION_IDS.lastEntry().getValue();

        // see comment on IDS field
        // now we're registered the index versions, we can clear the map
        IDS = null;
    }

    static NavigableMap<Integer, IndexVersion> getAllVersionIds(Class<?> cls) {
        Map<Integer, String> versionIdFields = new HashMap<>();
        NavigableMap<Integer, IndexVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("ZERO", "MINIMUM_COMPATIBLE", "MINIMUM_READONLY_COMPATIBLE");

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
                            + "]. Each IndexVersion should have a different version number";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    static Collection<IndexVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    static final IntFunction<String> VERSION_LOOKUP = ReleaseVersions.generateVersionsLookup(IndexVersions.class, LATEST_DEFINED.id());

    // no instance
    private IndexVersions() {}
}
