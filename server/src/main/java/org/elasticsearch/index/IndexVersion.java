/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.util.Version;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * The index version.
 * <p>
 * Prior to 8.8.0, the node {@link Version} was used everywhere. This class separates the index format version
 * from the running node version.
 */
@SuppressWarnings("checkstyle:linelength")
public final class IndexVersion implements Comparable<IndexVersion> {
    public static final IndexVersion ZERO = new IndexVersion(0, Version.LATEST, "00000000-0000-0000-0000-000000000000");
    public static final IndexVersion V_7_0_0 = new IndexVersion(7_00_00_99, Version.LUCENE_8_0_0);
    public static final IndexVersion V_7_0_1 = new IndexVersion(7_00_01_99, Version.LUCENE_8_0_0);
    public static final IndexVersion V_7_1_0 = new IndexVersion(7_01_00_99, Version.LUCENE_8_0_0);
    public static final IndexVersion V_7_1_1 = new IndexVersion(7_01_01_99, Version.LUCENE_8_0_0);
    public static final IndexVersion V_7_2_0 = new IndexVersion(7_02_00_99, Version.LUCENE_8_0_0);
    public static final IndexVersion V_7_2_1 = new IndexVersion(7_02_01_99, Version.LUCENE_8_0_0);
    public static final IndexVersion V_7_3_0 = new IndexVersion(7_03_00_99, Version.LUCENE_8_1_0);
    public static final IndexVersion V_7_3_1 = new IndexVersion(7_03_01_99, Version.LUCENE_8_1_0);
    public static final IndexVersion V_7_3_2 = new IndexVersion(7_03_02_99, Version.LUCENE_8_1_0);
    public static final IndexVersion V_7_4_0 = new IndexVersion(7_04_00_99, Version.LUCENE_8_2_0);
    public static final IndexVersion V_7_4_1 = new IndexVersion(7_04_01_99, Version.LUCENE_8_2_0);
    public static final IndexVersion V_7_4_2 = new IndexVersion(7_04_02_99, Version.LUCENE_8_2_0);
    public static final IndexVersion V_7_5_0 = new IndexVersion(7_05_00_99, Version.LUCENE_8_3_0);
    public static final IndexVersion V_7_5_1 = new IndexVersion(7_05_01_99, Version.LUCENE_8_3_0);
    public static final IndexVersion V_7_5_2 = new IndexVersion(7_05_02_99, Version.LUCENE_8_3_0);
    public static final IndexVersion V_7_6_0 = new IndexVersion(7_06_00_99, Version.LUCENE_8_4_0);
    public static final IndexVersion V_7_6_1 = new IndexVersion(7_06_01_99, Version.LUCENE_8_4_0);
    public static final IndexVersion V_7_6_2 = new IndexVersion(7_06_02_99, Version.LUCENE_8_4_0);
    public static final IndexVersion V_7_7_0 = new IndexVersion(7_07_00_99, Version.LUCENE_8_5_1);
    public static final IndexVersion V_7_7_1 = new IndexVersion(7_07_01_99, Version.LUCENE_8_5_1);
    public static final IndexVersion V_7_8_0 = new IndexVersion(7_08_00_99, Version.LUCENE_8_5_1);
    public static final IndexVersion V_7_8_1 = new IndexVersion(7_08_01_99, Version.LUCENE_8_5_1);
    public static final IndexVersion V_7_9_0 = new IndexVersion(7_09_00_99, Version.LUCENE_8_6_0);
    public static final IndexVersion V_7_9_1 = new IndexVersion(7_09_01_99, Version.LUCENE_8_6_2);
    public static final IndexVersion V_7_9_2 = new IndexVersion(7_09_02_99, Version.LUCENE_8_6_2);
    public static final IndexVersion V_7_9_3 = new IndexVersion(7_09_03_99, Version.LUCENE_8_6_2);
    public static final IndexVersion V_7_10_0 = new IndexVersion(7_10_00_99, Version.LUCENE_8_7_0);
    public static final IndexVersion V_7_10_1 = new IndexVersion(7_10_01_99, Version.LUCENE_8_7_0);
    public static final IndexVersion V_7_10_2 = new IndexVersion(7_10_02_99, Version.LUCENE_8_7_0);
    public static final IndexVersion V_7_11_0 = new IndexVersion(7_11_00_99, Version.LUCENE_8_7_0);
    public static final IndexVersion V_7_11_1 = new IndexVersion(7_11_01_99, Version.LUCENE_8_7_0);
    public static final IndexVersion V_7_11_2 = new IndexVersion(7_11_02_99, Version.LUCENE_8_7_0);
    public static final IndexVersion V_7_12_0 = new IndexVersion(7_12_00_99, Version.LUCENE_8_8_0);
    public static final IndexVersion V_7_12_1 = new IndexVersion(7_12_01_99, Version.LUCENE_8_8_0);
    public static final IndexVersion V_7_13_0 = new IndexVersion(7_13_00_99, Version.LUCENE_8_8_2);
    public static final IndexVersion V_7_13_1 = new IndexVersion(7_13_01_99, Version.LUCENE_8_8_2);
    public static final IndexVersion V_7_13_2 = new IndexVersion(7_13_02_99, Version.LUCENE_8_8_2);
    public static final IndexVersion V_7_13_3 = new IndexVersion(7_13_03_99, Version.LUCENE_8_8_2);
    public static final IndexVersion V_7_13_4 = new IndexVersion(7_13_04_99, Version.LUCENE_8_8_2);
    public static final IndexVersion V_7_14_0 = new IndexVersion(7_14_00_99, Version.LUCENE_8_9_0);
    public static final IndexVersion V_7_14_1 = new IndexVersion(7_14_01_99, Version.LUCENE_8_9_0);
    public static final IndexVersion V_7_14_2 = new IndexVersion(7_14_02_99, Version.LUCENE_8_9_0);
    public static final IndexVersion V_7_15_0 = new IndexVersion(7_15_00_99, Version.LUCENE_8_9_0);
    public static final IndexVersion V_7_15_1 = new IndexVersion(7_15_01_99, Version.LUCENE_8_9_0);
    public static final IndexVersion V_7_15_2 = new IndexVersion(7_15_02_99, Version.LUCENE_8_9_0);
    public static final IndexVersion V_7_16_0 = new IndexVersion(7_16_00_99, Version.LUCENE_8_10_1);
    public static final IndexVersion V_7_16_1 = new IndexVersion(7_16_01_99, Version.LUCENE_8_10_1);
    public static final IndexVersion V_7_16_2 = new IndexVersion(7_16_02_99, Version.LUCENE_8_10_1);
    public static final IndexVersion V_7_16_3 = new IndexVersion(7_16_03_99, Version.LUCENE_8_10_1);
    public static final IndexVersion V_7_17_0 = new IndexVersion(7_17_00_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_1 = new IndexVersion(7_17_01_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_2 = new IndexVersion(7_17_02_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_3 = new IndexVersion(7_17_03_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_4 = new IndexVersion(7_17_04_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_5 = new IndexVersion(7_17_05_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_6 = new IndexVersion(7_17_06_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_7 = new IndexVersion(7_17_07_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_8 = new IndexVersion(7_17_08_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_9 = new IndexVersion(7_17_09_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_7_17_10 = new IndexVersion(7_17_10_99, Version.LUCENE_8_11_1);
    public static final IndexVersion V_8_0_0 = new IndexVersion(8_00_00_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_0_1 = new IndexVersion(8_00_01_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_1_0 = new IndexVersion(8_01_00_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_1_1 = new IndexVersion(8_01_01_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_1_2 = new IndexVersion(8_01_02_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_1_3 = new IndexVersion(8_01_03_99, Version.LUCENE_9_0_0);
    public static final IndexVersion V_8_2_0 = new IndexVersion(8_02_00_99, Version.LUCENE_9_1_0);
    public static final IndexVersion V_8_2_1 = new IndexVersion(8_02_01_99, Version.LUCENE_9_1_0);
    public static final IndexVersion V_8_2_2 = new IndexVersion(8_02_02_99, Version.LUCENE_9_1_0);
    public static final IndexVersion V_8_2_3 = new IndexVersion(8_02_03_99, Version.LUCENE_9_1_0);
    public static final IndexVersion V_8_3_0 = new IndexVersion(8_03_00_99, Version.LUCENE_9_2_0);
    public static final IndexVersion V_8_3_1 = new IndexVersion(8_03_01_99, Version.LUCENE_9_2_0);
    public static final IndexVersion V_8_3_2 = new IndexVersion(8_03_02_99, Version.LUCENE_9_2_0);
    public static final IndexVersion V_8_3_3 = new IndexVersion(8_03_03_99, Version.LUCENE_9_2_0);
    public static final IndexVersion V_8_4_0 = new IndexVersion(8_04_00_99, Version.LUCENE_9_3_0);
    public static final IndexVersion V_8_4_1 = new IndexVersion(8_04_01_99, Version.LUCENE_9_3_0);
    public static final IndexVersion V_8_4_2 = new IndexVersion(8_04_02_99, Version.LUCENE_9_3_0);
    public static final IndexVersion V_8_4_3 = new IndexVersion(8_04_03_99, Version.LUCENE_9_3_0);
    public static final IndexVersion V_8_5_0 = new IndexVersion(8_05_00_99, Version.LUCENE_9_4_1);
    public static final IndexVersion V_8_5_1 = new IndexVersion(8_05_01_99, Version.LUCENE_9_4_1);
    public static final IndexVersion V_8_5_2 = new IndexVersion(8_05_02_99, Version.LUCENE_9_4_1);
    public static final IndexVersion V_8_5_3 = new IndexVersion(8_05_03_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_6_0 = new IndexVersion(8_06_00_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_6_1 = new IndexVersion(8_06_01_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_6_2 = new IndexVersion(8_06_02_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_6_3 = new IndexVersion(8_06_03_99, Version.LUCENE_9_4_2);
    public static final IndexVersion V_8_7_0 = new IndexVersion(8_07_00_99, Version.LUCENE_9_5_0);

    public static final IndexVersion V_8_8_0 = new IndexVersion(8_08_00_99, Version.LUCENE_9_6_0);

    public static final IndexVersion CURRENT = V_8_8_0;

    public static final IndexVersion MINIMUM_COMPATIBLE = V_7_0_0;

    static NavigableMap<Integer, IndexVersion> getAllVersionIds(Class<?> cls) {
        NavigableMap<Integer, IndexVersion> builder = new TreeMap<>();
        Map<String, IndexVersion> uniqueIds = new HashMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");
        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(TransportVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }
                try {
                    IndexVersion version = (IndexVersion) declaredField.get(null);

                    IndexVersion maybePrevious = builder.put(version.id, version);
                    assert maybePrevious == null
                        : "expected [" + version.id + "] to be uniquely mapped but saw [" + maybePrevious + "] and [" + version + "]";

                    IndexVersion sameUniqueId = uniqueIds.put(version.uniqueId, version);
                    assert sameUniqueId == null
                        : "Versions "
                        + version
                        + " and "
                        + sameUniqueId
                        + " have the same unique id. Each IndexVersion should have a different unique id";
                } catch (IllegalAccessException e) {
                    assert false : "IndexVersion field [" + fieldName + "] should be public";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, IndexVersion> VERSION_IDS;

    static {
        VERSION_IDS = getAllVersionIds(IndexVersion.class);
    }

    static Collection<IndexVersion> getAllVersions() {
        return VERSION_IDS.values();
    }

    public final int id;
    public final Version luceneVersion;
    private final String uniqueId;

    IndexVersion(int id, Version luceneVersion, String uniqueId) {
        this.id = id;
        this.luceneVersion = Objects.requireNonNull(luceneVersion);
        this.uniqueId = Strings.requireNonEmpty(uniqueId, "Each IndexVersion needs a unique string id");
    }

    public static IndexVersion fromId(int id) {
        IndexVersion known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }
        // this is a version we don't otherwise know about - just create a placeholder
        // We need to guess the lucene version.
        // Our best guess is to use the same lucene version as the previous
        // version in the list, assuming that it didn't change.

        return new IndexVersion(id, "<unknown>");
    }

    @Deprecated(forRemoval = true)
    public org.elasticsearch.Version toVersion() {
        return org.elasticsearch.Version.fromId(id);
    }

    public boolean after(IndexVersion version) {
        return version.id < id;
    }

    public boolean onOrAfter(IndexVersion version) {
        return version.id <= id;
    }

    public boolean before(IndexVersion version) {
        return version.id > id;
    }

    public boolean onOrBefore(IndexVersion version) {
        return version.id >= id;
    }

    public boolean isLegacyIndexFormatVersion() {
        return before(MINIMUM_COMPATIBLE);
    }

    @Override
    public int compareTo(IndexVersion other) {
        return Integer.compare(this.id, other.id);
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexVersion version = (IndexVersion) o;

        if (id != version.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
