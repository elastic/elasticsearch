/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EsqlVersionTests extends ESTestCase {
    public void testVersionString() {
        assertThat(EsqlVersion.NIGHTLY.toString(), equalTo("nightly.😴"));
        assertThat(EsqlVersion.PARTY_POPPER.toString(), equalTo("2024.04.01.🎉"));
    }

    public void testVersionId() {
        assertThat(EsqlVersion.NIGHTLY.id(), equalTo(Integer.MAX_VALUE));
        assertThat(EsqlVersion.PARTY_POPPER.id(), equalTo(20240401));
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

        assertNull(EsqlVersion.parse(invalidVersionString()));
    }

    public static String invalidVersionString() {
        String[] invalidVersionString = new String[1];

        do {
            int length = randomIntBetween(0, 10);
            invalidVersionString[0] = randomAlphaOfLength(length);
        } while (EsqlVersion.VERSION_MAP_WITH_AND_WITHOUT_EMOJI.containsKey(invalidVersionString[0]));

        return invalidVersionString[0];
    }
}
