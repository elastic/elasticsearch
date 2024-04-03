/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.VersionId;

import java.util.LinkedHashMap;
import java.util.Map;

public enum EsqlVersion implements VersionId<EsqlVersion> {
    /**
     * Breaking changes go here until the next version is released.
     */
    SNAPSHOT(Integer.MAX_VALUE, 12, 99, "📷"),
    PARTY_POPPER(2024, 4, "🎉");

    static final Map<String, EsqlVersion> VERSION_MAP_WITH_AND_WITHOUT_EMOJI = versionMapWithAndWithoutEmoji();

    private static Map<String, EsqlVersion> versionMapWithAndWithoutEmoji() {
        Map<String, EsqlVersion> stringToVersion = new LinkedHashMap<>(EsqlVersion.values().length * 2);

        for (EsqlVersion version : EsqlVersion.values()) {
            putVersionAssertNoDups(stringToVersion, version.versionStringWithoutEmoji(), version);
            putVersionAssertNoDups(stringToVersion, version.toString(), version);
        }

        return stringToVersion;
    }

    private static void putVersionAssertNoDups(Map<String, EsqlVersion> stringToVersion, String versionString, EsqlVersion version) {
        EsqlVersion existingVersionForKey = stringToVersion.put(versionString, version);
        if (existingVersionForKey != null) {
            throw new AssertionError("Duplicate esql version with version string [" + versionString + "]");
        }
    }

    EsqlVersion(int year, int month, String emoji) {
        this(year, month, 1, emoji);
    }

    EsqlVersion(int year, int month, int revision, String emoji) {
        if ((1 <= revision && revision <= 99) == false) {
            throw new AssertionError("Version revision number must be between 1 and 99 but was [" + revision + "]");
        }
        if ((1 <= month && month <= 12) == false) {
            throw new AssertionError("Version month must be between 1 and 12 but was [" + month + "]");
        }
        if ((emoji.codePointCount(0, emoji.length()) == 1) == false) {
            throw new AssertionError("Version emoji must be a single unicode character but was [" + emoji + "]");
        }
        this.year = year;
        this.month = (byte) month;
        this.revision = (byte) revision;
        this.emoji = emoji;
    }

    private int year;
    private byte month;
    private byte revision;
    private String emoji;

    public int year() {
        return year;
    }

    public byte month() {
        return month;
    }

    public byte revision() {
        return revision;
    }

    public String emoji() {
        return emoji;
    }

    /**
     * Accepts a version string with the emoji suffix or without it.
     * E.g. both "2024.04.01.🎉" and "2024.04.01" will be interpreted as {@link EsqlVersion#PARTY_POPPER}.
     */
    public static EsqlVersion parse(String versionString) {
        return VERSION_MAP_WITH_AND_WITHOUT_EMOJI.get(versionString);
    }

    public String versionStringWithoutEmoji() {
        return this == SNAPSHOT ? "snapshot" : Strings.format("%d.%02d.%02d", year, month, revision);
    }

    @Override
    public String toString() {
        return versionStringWithoutEmoji() + "." + emoji;
    }

    @Override
    public int id() {
        return this == SNAPSHOT ? Integer.MAX_VALUE : (10000 * year + 100 * month + revision);
    }
}
