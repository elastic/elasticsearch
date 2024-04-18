/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlVersionTests extends ESTestCase {
    public void testLatestReleased() {
        assertThat(EsqlVersion.latestReleased(), is(EsqlVersion.ROCKET));
    }

    public void testVersionString() {
        assertThat(EsqlVersion.SNAPSHOT.toString(), equalTo("snapshot.📷"));
        assertThat(EsqlVersion.ROCKET.toString(), equalTo("2024.04.01.🚀"));
    }

    public void testVersionId() {
        assertThat(EsqlVersion.SNAPSHOT.id(), equalTo(Integer.MAX_VALUE));
        assertThat(EsqlVersion.ROCKET.id(), equalTo(20240401));

        for (EsqlVersion version : EsqlVersion.values()) {
            assertTrue(EsqlVersion.SNAPSHOT.onOrAfter(version));
            if (version != EsqlVersion.SNAPSHOT) {
                assertTrue(version.before(EsqlVersion.SNAPSHOT));
            } else {
                assertTrue(version.onOrAfter(EsqlVersion.SNAPSHOT));
            }
        }

        List<EsqlVersion> versionsSortedAsc = Arrays.stream(EsqlVersion.values())
            .sorted(Comparator.comparing(EsqlVersion::year).thenComparing(EsqlVersion::month).thenComparing(EsqlVersion::revision))
            .toList();
        for (int i = 0; i < versionsSortedAsc.size() - 1; i++) {
            assertTrue(versionsSortedAsc.get(i).before(versionsSortedAsc.get(i + 1)));
        }
    }

    public void testVersionStringNoEmoji() {
        for (EsqlVersion version : EsqlVersion.values()) {
            String[] versionSegments = version.toString().split("\\.");
            String[] parsingPrefixSegments = Arrays.copyOf(versionSegments, versionSegments.length - 1);

            String expectedParsingPrefix = String.join(".", parsingPrefixSegments);
            assertThat(version.versionStringWithoutEmoji(), equalTo(expectedParsingPrefix));
        }
    }

    public void testParsing() {
        for (EsqlVersion version : EsqlVersion.values()) {
            String versionStringWithoutEmoji = version.versionStringWithoutEmoji();

            assertThat(EsqlVersion.parse(versionStringWithoutEmoji), is(version));
            assertThat(EsqlVersion.parse(versionStringWithoutEmoji + "." + version.emoji()), is(version));
        }

        assertNull(EsqlVersion.parse(randomInvalidVersionString()));
    }

    public static String randomInvalidVersionString() {
        String[] invalidVersionString = new String[1];

        do {
            int length = randomIntBetween(1, 10);
            invalidVersionString[0] = randomAlphaOfLength(length);
        } while (EsqlVersion.VERSION_MAP_WITH_AND_WITHOUT_EMOJI.containsKey(invalidVersionString[0]));

        return invalidVersionString[0];
    }
}
