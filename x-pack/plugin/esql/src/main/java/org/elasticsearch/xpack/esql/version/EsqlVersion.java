/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.VersionId;

public enum EsqlVersion implements VersionId<EsqlVersion> {
    // Breaking changes go here until the next version is released.
    NIGHTLY(Integer.MAX_VALUE, Integer.MAX_VALUE, "😴"),
    PARTY_POPPER(2024, 4, "🎉");

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
    public String versionStringNoEmoji() {
        return this == NIGHTLY ? "nightly" : Strings.format("%d.%02d.%02d", year, month, numberThisMonth);
    }

    public static EsqlVersion parse(String versionString) {
        EsqlVersion parsed = null;
        if (Strings.hasText(versionString)) {
            versionString = Strings.toLowercaseAscii(versionString);
            for (EsqlVersion version : EsqlVersion.values()) {
                if (versionString.startsWith(version.versionStringNoEmoji())) {
                    return version;
                }
            }
        }
        return parsed;
    }

    @Override
    public String toString() {
        return versionStringNoEmoji() + "." + emoji;
    }

    @Override
    public int id() {
        return this == NIGHTLY ? Integer.MAX_VALUE : (10000 * year + 100 * month + numberThisMonth);
    }
}
