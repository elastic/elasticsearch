/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.Version;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * The index format version.
 * <p>
 * Prior to 8.7.0, the node {@link Version} was used everywhere. This class separates the index format version
 * from the running node version.
 */
public final class IndexFormatVersion implements Comparable<IndexFormatVersion> {
    public static final IndexFormatVersion ZERO = new IndexFormatVersion(0);
    public static final IndexFormatVersion V_7_0_0 = new IndexFormatVersion(7_00_00_99);
    public static final IndexFormatVersion V_7_2_0 = new IndexFormatVersion(7_02_00_99);
    public static final IndexFormatVersion V_8_0_0 = new IndexFormatVersion(8_00_00_99);
    public static final IndexFormatVersion V_8_5_0 = new IndexFormatVersion(8_05_00_99);
    public static final IndexFormatVersion V_8_8_0 = new IndexFormatVersion(8_08_00_99);

    public static final IndexFormatVersion CURRENT = V_8_8_0;

    public static final IndexFormatVersion MINIMUM_COMPATIBLE = V_7_0_0;

    static NavigableMap<Integer, IndexFormatVersion> getAllVersionIds(Class<?> cls) {
        NavigableMap<Integer, IndexFormatVersion> builder = new TreeMap<>();

        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");
        for (Field declaredField : cls.getFields()) {
            if (declaredField.getType().equals(IndexFormatVersion.class)) {
                String fieldName = declaredField.getName();
                if (ignore.contains(fieldName)) {
                    continue;
                }
                try {
                    IndexFormatVersion version = (IndexFormatVersion) declaredField.get(null);

                    IndexFormatVersion maybePrevious = builder.put(version.id, version);
                    assert maybePrevious == null
                        : "expected [" + version.id + "] to be uniquely mapped but saw [" + maybePrevious + "] and [" + version + "]";
                } catch (IllegalAccessException e) {
                    assert false : "IndexFormatVersion field [" + fieldName + "] should be public";
                }
            }
        }

        return Collections.unmodifiableNavigableMap(builder);
    }

    private static final NavigableMap<Integer, IndexFormatVersion> VERSION_IDS;

    static {
        VERSION_IDS = getAllVersionIds(IndexFormatVersion.class);
    }

    public final int id;

    IndexFormatVersion(int id) {
        this.id = id;
    }

    public static IndexFormatVersion fromId(int id) {
        IndexFormatVersion known = VERSION_IDS.get(id);
        if (known != null) {
            return known;
        }
        // this is a version we don't otherwise know about - just create a placeholder
        return new IndexFormatVersion(id);
    }

    @Deprecated(forRemoval = true)
    public Version toVersion() {
        return Version.fromId(id);
    }

    @Deprecated(forRemoval = true)
    public static IndexFormatVersion fromVersion(Version version) {
        return IndexFormatVersion.fromId(version.id);
    }

    public boolean after(IndexFormatVersion version) {
        return version.id < id;
    }

    public boolean onOrAfter(IndexFormatVersion version) {
        return version.id <= id;
    }

    public boolean before(IndexFormatVersion version) {
        return version.id > id;
    }

    public boolean onOrBefore(IndexFormatVersion version) {
        return version.id >= id;
    }

    public boolean isLegacyIndexFormatVersion() {
        return before(MINIMUM_COMPATIBLE);
    }

    @Override
    public int compareTo(IndexFormatVersion other) {
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

        IndexFormatVersion version = (IndexFormatVersion) o;

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
