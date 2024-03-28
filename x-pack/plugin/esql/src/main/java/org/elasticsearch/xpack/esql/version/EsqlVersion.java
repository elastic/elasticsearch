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
    NIGHTLY(Integer.MAX_VALUE, Integer.MAX_VALUE, "😴"),
    PARTY_POPPER(2024, 4, "🎉");

    static final Map<String, EsqlVersion> VERSION_MAP_WITH_AND_WITHOUT_EMOJI = versionMapWithAndWithoutEmoji();

    private static Map<String, EsqlVersion> versionMapWithAndWithoutEmoji() {
        Map<String, EsqlVersion> stringToVersion = new LinkedHashMap<>(EsqlVersion.values().length * 2);

        for (EsqlVersion version : EsqlVersion.values()) {
            EsqlVersion existingVersionForKey = null;
            existingVersionForKey = stringToVersion.put(version.versionStringWithoutEmoji(), version);
            assert existingVersionForKey == null;
            existingVersionForKey = stringToVersion.put(version.toString(), version);
            assert existingVersionForKey == null;
        }

        return stringToVersion;
    }

    EsqlVersion(int year, int month, String emoji) {
        this(year, month, 1, emoji);
    }

    EsqlVersion(int year, int month, int numberThisMonth, String emoji) {
        assert 0 < numberThisMonth && numberThisMonth < 100;
        this.year = year;
        this.month = month;
        this.numberThisMonth = numberThisMonth;
        this.emoji = emoji;
    }

    private int year;
    private int month;
    // In case we really have to release more than one version in a given month, disambiguates between versions in the month.
    private int numberThisMonth;
    private String emoji;

    /**
     * Version prefix that we accept when parsing. If a version string starts with the given prefix, we consider the version string valid.
     * E.g. "2024.04.01.🎉" will be interpreted as {@link EsqlVersion#PARTY_POPPER}, but so will "2024.04.01".
     */
    public String versionStringWithoutEmoji() {
        return this == NIGHTLY ? "nightly" : Strings.format("%d.%02d.%02d", year, month, numberThisMonth);
    }

    public String emoji() {
        return emoji;
    }

    public static EsqlVersion parse(String versionString) {
        return VERSION_MAP_WITH_AND_WITHOUT_EMOJI.get(versionString);
    }

    @Override
    public String toString() {
        return versionStringWithoutEmoji() + "." + emoji;
    }

    @Override
    public int id() {
        return this == NIGHTLY ? Integer.MAX_VALUE : (10000 * year + 100 * month + numberThisMonth);
    }
}
