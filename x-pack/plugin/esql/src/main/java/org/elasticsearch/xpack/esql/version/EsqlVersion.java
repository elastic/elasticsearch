/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The version of the ESQL language being processed.
 * <p>
 *     ESQL is a young language and we don't have the benefit of self-hosting
 *     its compiler. So we're going to make a lot of mistakes when designing it.
 *     As such, we expect it to change in backwards incompatible ways several
 *     times in 2024 and 2025. Hopefully we'll have learned our lesson and we'll
 *     settle down to one change every couple of years after that.
 * </p>
 * <p>
 *     For example, maybe we realize we've made a mistake with the {@link MvAvg}
 *     function and decide it should return the type of its input field rather
 *     than always returning a {@code double}. If we decide to make this change
 *     we'd have to bump the language version. We plan to batch changes like this
 *     into the {@link EsqlVersion#SNAPSHOT} version for a while and from time to
 *     time release them as a new version.
 * </p>
 * <p>
 *     We require a version to be sent on every request to the ESQL APIs so
 *     changing the version of a query is always opt-in. There is no REST request
 *     you can send to any ESQL endpoint that will default to a version of ESQL.
 *     That means we can release new versions of ESQL in a minor release of
 *     Elasticsearch. We can and we will.
 * </p>
 * <p>
 *     So users of Elasticsearch's clients don't need to think about the version
 *     of ESQL when they are getting started they we have a concept of "base version".
 *     This "base version" will remain constant for an entire major release of
 *     Elasticsearch and clients will send that version with ESQL requests unless
 *     otherwise configured.
 * </p>
 * <p>
 *     This is marked with {@link UpdateForV9} to remind us that we need to
 *     update the "base version" of ESQL in the client specification when
 *     we cut a new major. We'll need to do that on every major - and also bump the {@link UpdateForV9} annotation.
 * </p>
 */
public enum EsqlVersion implements VersionId<EsqlVersion> {
    /**
     * Breaking changes go here until the next version is released.
     */
    SNAPSHOT(Integer.MAX_VALUE, 12, 99, "📷"),
    ROCKET(2024, 4, "🚀");

    static final Map<String, EsqlVersion> VERSION_MAP_WITH_AND_WITHOUT_EMOJI = versionMapWithAndWithoutEmoji();
    private static final EsqlVersion[] RELEASED_ASCENDING = createReleasedAscending();

    private static Map<String, EsqlVersion> versionMapWithAndWithoutEmoji() {
        Map<String, EsqlVersion> stringToVersion = new LinkedHashMap<>(EsqlVersion.values().length * 2);

        for (EsqlVersion version : EsqlVersion.values()) {
            putVersionCheckNoDups(stringToVersion, version.versionStringWithoutEmoji(), version);
            putVersionCheckNoDups(stringToVersion, version.toString(), version);
        }

        return stringToVersion;
    }

    private static EsqlVersion[] createReleasedAscending() {
        return Arrays.stream(EsqlVersion.values())
            .filter(v -> v != SNAPSHOT)
            .sorted(Comparator.comparingInt(EsqlVersion::id))
            .toArray(EsqlVersion[]::new);
    }

    private static void putVersionCheckNoDups(Map<String, EsqlVersion> stringToVersion, String versionString, EsqlVersion version) {
        EsqlVersion existingVersionForKey = stringToVersion.put(versionString, version);
        if (existingVersionForKey != null) {
            throw new IllegalArgumentException("Duplicate esql version with version string [" + versionString + "]");
        }
    }

    /**
     * Accepts a version string with the emoji suffix or without it.
     * E.g. both "2024.04.01.🚀" and "2024.04.01" will be interpreted as {@link EsqlVersion#ROCKET}.
     */
    public static EsqlVersion parse(String versionString) {
        return VERSION_MAP_WITH_AND_WITHOUT_EMOJI.get(versionString);
    }

    /**
     * Return the released versions in ascending order.
     */
    public static EsqlVersion[] releasedAscending() {
        return RELEASED_ASCENDING;
    }

    public static EsqlVersion latestReleased() {
        return RELEASED_ASCENDING[RELEASED_ASCENDING.length - 1];
    }

    private int year;
    private byte month;
    private byte revision;
    private String emoji;

    EsqlVersion(int year, int month, String emoji) {
        this(year, month, 1, emoji);
    }

    EsqlVersion(int year, int month, int revision, String emoji) {
        if ((1 <= revision && revision <= 99) == false) {
            throw new IllegalArgumentException("Version revision number must be between 1 and 99 but was [" + revision + "]");
        }
        if ((1 <= month && month <= 12) == false) {
            throw new IllegalArgumentException("Version month must be between 1 and 12 but was [" + month + "]");
        }
        if ((emoji.codePointCount(0, emoji.length()) == 1) == false) {
            throw new IllegalArgumentException("Version emoji must be a single unicode character but was [" + emoji + "]");
        }
        this.year = year;
        this.month = (byte) month;
        this.revision = (byte) revision;
        this.emoji = emoji;
    }

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
