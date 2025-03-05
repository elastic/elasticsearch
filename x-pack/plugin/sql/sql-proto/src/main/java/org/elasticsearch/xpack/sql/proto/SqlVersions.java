/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.sql.proto.SqlVersion.fromId;

public final class SqlVersions {

    public static final SqlVersion V_7_0_0 = fromId(7_00_00_99);
    public static final SqlVersion V_7_0_1 = fromId(7_00_01_99);
    public static final SqlVersion V_7_1_0 = fromId(7_01_00_99);
    public static final SqlVersion V_7_1_1 = fromId(7_01_01_99);
    public static final SqlVersion V_7_2_0 = fromId(7_02_00_99);
    public static final SqlVersion V_7_2_1 = fromId(7_02_01_99);
    public static final SqlVersion V_7_3_0 = fromId(7_03_00_99);
    public static final SqlVersion V_7_3_1 = fromId(7_03_01_99);
    public static final SqlVersion V_7_3_2 = fromId(7_03_02_99);
    public static final SqlVersion V_7_4_0 = fromId(7_04_00_99);
    public static final SqlVersion V_7_4_1 = fromId(7_04_01_99);
    public static final SqlVersion V_7_4_2 = fromId(7_04_02_99);
    public static final SqlVersion V_7_5_0 = fromId(7_05_00_99);
    public static final SqlVersion V_7_5_1 = fromId(7_05_01_99);
    public static final SqlVersion V_7_5_2 = fromId(7_05_02_99);
    public static final SqlVersion V_7_6_0 = fromId(7_06_00_99);
    public static final SqlVersion V_7_6_1 = fromId(7_06_01_99);
    public static final SqlVersion V_7_6_2 = fromId(7_06_02_99);
    public static final SqlVersion V_7_7_0 = fromId(7_07_00_99);
    public static final SqlVersion V_7_7_1 = fromId(7_07_01_99);
    public static final SqlVersion V_7_8_0 = fromId(7_08_00_99);
    public static final SqlVersion V_7_8_1 = fromId(7_08_01_99);
    public static final SqlVersion V_7_9_0 = fromId(7_09_00_99);
    public static final SqlVersion V_7_9_1 = fromId(7_09_01_99);
    public static final SqlVersion V_7_9_2 = fromId(7_09_02_99);
    public static final SqlVersion V_7_9_3 = fromId(7_09_03_99);
    public static final SqlVersion V_7_10_0 = fromId(7_10_00_99);
    public static final SqlVersion V_7_10_1 = fromId(7_10_01_99);
    public static final SqlVersion V_7_10_2 = fromId(7_10_02_99);
    public static final SqlVersion V_7_11_0 = fromId(7_11_00_99);
    public static final SqlVersion V_7_11_1 = fromId(7_11_01_99);
    public static final SqlVersion V_7_11_2 = fromId(7_11_02_99);
    public static final SqlVersion V_7_12_0 = fromId(7_12_00_99);
    public static final SqlVersion V_7_12_1 = fromId(7_12_01_99);
    public static final SqlVersion V_7_13_0 = fromId(7_13_00_99);
    public static final SqlVersion V_7_13_1 = fromId(7_13_01_99);
    public static final SqlVersion V_7_13_2 = fromId(7_13_02_99);
    public static final SqlVersion V_7_13_3 = fromId(7_13_03_99);
    public static final SqlVersion V_7_13_4 = fromId(7_13_04_99);
    public static final SqlVersion V_7_14_0 = fromId(7_14_00_99);
    public static final SqlVersion V_7_14_1 = fromId(7_14_01_99);
    public static final SqlVersion V_7_14_2 = fromId(7_14_02_99);
    public static final SqlVersion V_7_15_0 = fromId(7_15_00_99);
    public static final SqlVersion V_7_15_1 = fromId(7_15_01_99);
    public static final SqlVersion V_7_15_2 = fromId(7_15_02_99);
    public static final SqlVersion V_7_16_0 = fromId(7_16_00_99);
    public static final SqlVersion V_7_16_1 = fromId(7_16_01_99);
    public static final SqlVersion V_7_16_2 = fromId(7_16_02_99);
    public static final SqlVersion V_7_16_3 = fromId(7_16_03_99);
    public static final SqlVersion V_7_17_0 = fromId(7_17_00_99);
    public static final SqlVersion V_7_17_1 = fromId(7_17_01_99);
    public static final SqlVersion V_7_17_2 = fromId(7_17_02_99);
    public static final SqlVersion V_7_17_3 = fromId(7_17_03_99);
    public static final SqlVersion V_7_17_4 = fromId(7_17_04_99);
    public static final SqlVersion V_7_17_5 = fromId(7_17_05_99);
    public static final SqlVersion V_7_17_6 = fromId(7_17_06_99);
    public static final SqlVersion V_7_17_7 = fromId(7_17_07_99);
    public static final SqlVersion V_7_17_8 = fromId(7_17_08_99);
    public static final SqlVersion V_7_17_9 = fromId(7_17_09_99);
    public static final SqlVersion V_7_17_10 = fromId(7_17_10_99);
    public static final SqlVersion V_7_17_11 = fromId(7_17_11_99);
    public static final SqlVersion V_7_17_12 = fromId(7_17_12_99);
    public static final SqlVersion V_7_17_13 = fromId(7_17_13_99);
    public static final SqlVersion V_7_17_14 = fromId(7_17_14_99);
    public static final SqlVersion V_7_17_15 = fromId(7_17_15_99);
    public static final SqlVersion V_7_17_16 = fromId(7_17_16_99);
    public static final SqlVersion V_7_17_17 = fromId(7_17_17_99);
    public static final SqlVersion V_7_17_18 = fromId(7_17_18_99);
    public static final SqlVersion V_7_17_19 = fromId(7_17_19_99);
    public static final SqlVersion V_7_17_20 = fromId(7_17_20_99);
    public static final SqlVersion V_7_17_21 = fromId(7_17_21_99);
    public static final SqlVersion V_7_17_22 = fromId(7_17_22_99);
    public static final SqlVersion V_7_17_23 = fromId(7_17_23_99);
    public static final SqlVersion V_7_17_24 = fromId(7_17_24_99);

    public static final SqlVersion V_8_0_0 = fromId(8_00_00_99);
    public static final SqlVersion V_8_0_1 = fromId(8_00_01_99);
    public static final SqlVersion V_8_1_0 = fromId(8_01_00_99);
    public static final SqlVersion V_8_1_1 = fromId(8_01_01_99);
    public static final SqlVersion V_8_1_2 = fromId(8_01_02_99);
    public static final SqlVersion V_8_1_3 = fromId(8_01_03_99);
    public static final SqlVersion V_8_2_0 = fromId(8_02_00_99);
    public static final SqlVersion V_8_2_1 = fromId(8_02_01_99);
    public static final SqlVersion V_8_2_2 = fromId(8_02_02_99);
    public static final SqlVersion V_8_2_3 = fromId(8_02_03_99);
    public static final SqlVersion V_8_3_0 = fromId(8_03_00_99);
    public static final SqlVersion V_8_3_1 = fromId(8_03_01_99);
    public static final SqlVersion V_8_3_2 = fromId(8_03_02_99);
    public static final SqlVersion V_8_3_3 = fromId(8_03_03_99);
    public static final SqlVersion V_8_4_0 = fromId(8_04_00_99);
    public static final SqlVersion V_8_4_1 = fromId(8_04_01_99);
    public static final SqlVersion V_8_4_2 = fromId(8_04_02_99);
    public static final SqlVersion V_8_4_3 = fromId(8_04_03_99);
    public static final SqlVersion V_8_5_0 = fromId(8_05_00_99);
    public static final SqlVersion V_8_5_1 = fromId(8_05_01_99);
    public static final SqlVersion V_8_5_2 = fromId(8_05_02_99);
    public static final SqlVersion V_8_5_3 = fromId(8_05_03_99);
    public static final SqlVersion V_8_6_0 = fromId(8_06_00_99);
    public static final SqlVersion V_8_6_1 = fromId(8_06_01_99);
    public static final SqlVersion V_8_6_2 = fromId(8_06_02_99);
    public static final SqlVersion V_8_7_0 = fromId(8_07_00_99);
    public static final SqlVersion V_8_7_1 = fromId(8_07_01_99);
    public static final SqlVersion V_8_8_0 = fromId(8_08_00_99);
    public static final SqlVersion V_8_8_1 = fromId(8_08_01_99);
    public static final SqlVersion V_8_8_2 = fromId(8_08_02_99);
    public static final SqlVersion V_8_9_0 = fromId(8_09_00_99);
    public static final SqlVersion V_8_9_1 = fromId(8_09_01_99);
    public static final SqlVersion V_8_9_2 = fromId(8_09_02_99);
    public static final SqlVersion V_8_10_0 = fromId(8_10_00_99);
    public static final SqlVersion V_8_10_1 = fromId(8_10_01_99);
    public static final SqlVersion V_8_10_2 = fromId(8_10_02_99);
    public static final SqlVersion V_8_10_3 = fromId(8_10_03_99);
    public static final SqlVersion V_8_10_4 = fromId(8_10_04_99);
    public static final SqlVersion V_8_11_0 = fromId(8_11_00_99);
    public static final SqlVersion V_8_11_1 = fromId(8_11_01_99);
    public static final SqlVersion V_8_11_2 = fromId(8_11_02_99);
    public static final SqlVersion V_8_11_3 = fromId(8_11_03_99);
    public static final SqlVersion V_8_11_4 = fromId(8_11_04_99);
    public static final SqlVersion V_8_12_0 = fromId(8_12_00_99);
    public static final SqlVersion V_8_12_1 = fromId(8_12_01_99);
    public static final SqlVersion V_8_12_2 = fromId(8_12_02_99);
    public static final SqlVersion V_8_13_0 = fromId(8_13_00_99);
    public static final SqlVersion V_8_13_1 = fromId(8_13_01_99);
    public static final SqlVersion V_8_13_2 = fromId(8_13_02_99);
    public static final SqlVersion V_8_13_3 = fromId(8_13_03_99);
    public static final SqlVersion V_8_13_4 = fromId(8_13_04_99);
    public static final SqlVersion V_8_14_0 = fromId(8_14_00_99);
    public static final SqlVersion V_8_14_1 = fromId(8_14_01_99);
    public static final SqlVersion V_8_14_2 = fromId(8_14_02_99);
    public static final SqlVersion V_8_14_3 = fromId(8_14_03_99);
    public static final SqlVersion V_8_15_0 = fromId(8_15_00_99);
    public static final SqlVersion V_8_15_1 = fromId(8_15_01_99);
    public static final SqlVersion V_8_16_0 = fromId(8_16_00_99);

    static final List<SqlVersion> DECLARED_VERSIONS = getDeclaredVersions();

    /**
     * What's the version of the server that the clients should be compatible with?
     */
    public static final SqlVersion SERVER_COMPAT_VERSION = getLatestVersion();

    public static SqlVersion getFirstVersion() {
        return DECLARED_VERSIONS.get(0);
    }

    public static SqlVersion getLatestVersion() {
        return DECLARED_VERSIONS.get(DECLARED_VERSIONS.size() - 1);
    }

    public static SqlVersion getPreviousVersion(SqlVersion version) {
        int index = Collections.binarySearch(DECLARED_VERSIONS, version);
        if (index < 1) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return DECLARED_VERSIONS.get(index - 1);
    }

    public static SqlVersion getNextVersion(SqlVersion version) {
        int index = Collections.binarySearch(DECLARED_VERSIONS, version);
        if (index >= DECLARED_VERSIONS.size() - 1) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return DECLARED_VERSIONS.get(index + 1);
    }

    public static List<SqlVersion> getAllVersions() {
        return DECLARED_VERSIONS;
    }

    // lifted from org.elasticsearch.Version#getDeclaredVersions
    private static List<SqlVersion> getDeclaredVersions() {
        final Field[] fields = SqlVersions.class.getFields();
        final List<SqlVersion> versions = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final int mod = field.getModifiers();
            if (false == (Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod))) {
                continue;
            }
            if (field.getType() != SqlVersion.class) {
                continue;
            }
            switch (field.getName()) {
                case "LATEST":
                case "SERVER_COMPAT_VERSION":
                    continue;
            }
            assert field.getName().matches("V(_\\d+){3}?") : field.getName();
            try {
                if (field.get(null) == null) {
                    throw new IllegalStateException("field " + field.getName() + " is null");
                }
                versions.add(((SqlVersion) field.get(null)));
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(versions);
        return versions;
    }
}
