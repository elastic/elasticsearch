/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class IsolatedProjectsTests extends ESTestCase {

    public void testIsolatedTierNameReturnsEmptyWhenParsedTiersObjectHasNoTierKeys() {
        String projectIdRaw = randomProjectIdString();
        IsolatedProjects isolatedProjects = IsolatedProjects.parse(isolatedProjectsEntryJson(projectIdRaw, "{}", ""));
        ProjectId projectId = ProjectId.fromId(projectIdRaw);
        assertFalse(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).isPresent());
        assertFalse(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).isPresent());
    }

    public void testIsolatedTierNameReturnsEmptyWhenProjectAbsentFromParsedMap() {
        String configuredProjectRaw = randomProjectIdString();
        String randomIsolatedTierName = randomValidIsolatedTierName();
        String unknownProjectRaw = randomValueOtherThan(configuredProjectRaw, IsolatedProjectsTests::randomProjectIdString);

        IsolatedProjects isolatedProjects = IsolatedProjects.parse(
            isolatedProjectsEntryJson(configuredProjectRaw, "{\"index\":\"" + randomIsolatedTierName + "\"}", "")
        );
        ProjectId unknownProjectId = ProjectId.fromId(unknownProjectRaw);
        assertFalse(isolatedProjects.isolatedTierName(unknownProjectId, IsolationShardTier.INDEX).isPresent());
        assertFalse(isolatedProjects.isolatedTierName(unknownProjectId, IsolationShardTier.SEARCH).isPresent());
    }

    public void testIsolatedTierNameReturnsTierNamesForIndexAndSearch() {
        String projectIdRaw = randomProjectIdString();
        String indexTier = randomValidIsolatedTierName();
        String searchTier = randomValidIsolatedTierName();
        IsolatedProjects isolatedProjects = IsolatedProjects.parse(
            isolatedProjectsEntryJson(
                projectIdRaw,
                String.format(Locale.ROOT, "{\"index\":\"%s\",\"search\":\"%s\"}", indexTier, searchTier),
                ""
            )
        );
        ProjectId projectId = ProjectId.fromId(projectIdRaw);
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).orElse(null), equalTo(indexTier));
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).orElse(null), equalTo(searchTier));
    }

    public void testParseMergesTierIsolationWhenSameProjectAppearsMultipleTimes() {
        String projectIdRaw = randomProjectIdString();
        String indexFirst = randomValidIsolatedTierName();
        String searchSecond = randomValidIsolatedTierName();
        String indexOverride = randomValidIsolatedTierName();

        IsolatedProjects isolatedProjects = IsolatedProjects.parse(String.format(Locale.ROOT, """
            [
              {"project":"%s","tiers":{"index":"%s"}},
              {"project":"%s","tiers":{"search":"%s","index":"%s"}}
            ]""", projectIdRaw, indexFirst, projectIdRaw, searchSecond, indexOverride));
        ProjectId projectId = ProjectId.fromId(projectIdRaw);
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).orElse(null), equalTo(indexOverride));
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).orElse(null), equalTo(searchSecond));
    }

    public void testParseAllowsTierNameAtMaximumLength() {
        String isolatedTierNameAtMaxLength = randomValidIsolatedTierNameExactly(IsolatedProjects.MAX_ISOLATED_TIER_NAME_LENGTH);
        String projectIdRaw = randomProjectIdString();

        IsolatedProjects isolatedProjects = IsolatedProjects.parse(
            isolatedProjectsEntryJson(
                projectIdRaw,
                String.format(
                    Locale.ROOT,
                    "{\"index\":\"%s\",\"search\":\"%s\"}",
                    isolatedTierNameAtMaxLength,
                    isolatedTierNameAtMaxLength
                ),
                ""
            )
        );

        ProjectId projectId = ProjectId.fromId(projectIdRaw);
        assertThat(
            isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).orElseThrow(),
            equalTo(isolatedTierNameAtMaxLength)
        );
        assertThat(
            isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).orElseThrow(),
            equalTo(isolatedTierNameAtMaxLength)
        );
    }

    public void testParseReturnsEmptyWhenRawIsNull() {
        assertSame(IsolatedProjects.EMPTY, IsolatedProjects.parse(null));
    }

    public void testParseReturnsEmptyWhenJsonIsEmptyArrayOrBlank() {
        assertSame(IsolatedProjects.EMPTY, IsolatedProjects.parse("[]"));
        assertSame(IsolatedProjects.EMPTY, IsolatedProjects.parse("  "));
    }

    public void testParseThrowsIllegalArgumentWhenEntryHasUnknownField() {
        String projectIdRaw = randomProjectIdString();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"" + projectIdRaw + "\",\"tiers\":{},\"x\":1}]")
        );
        assertThat(illegalArgumentException.getMessage().contains("unknown field"), equalTo(true));
    }

    public void testParseThrowsIllegalArgumentWhenTiersObjectHasUnknownField() {
        String projectIdRaw = randomProjectIdString();
        expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"" + projectIdRaw + "\",\"tiers\":{\"ml\":\"x\"}}]")
        );
    }

    public void testParseThrowsIllegalArgumentWhenTierNameHasIllegalCharacters() {
        String projectIdRaw = randomProjectIdString();
        String uppercaseIllegalTier = randomAlphaOfLength(randomIntBetween(3, IsolatedProjects.MAX_ISOLATED_TIER_NAME_LENGTH)).toUpperCase(
            Locale.ROOT
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse(isolatedProjectsEntryJson(projectIdRaw, "{\"index\":\"" + uppercaseIllegalTier + "\"}", ""))
        );
    }

    public void testParseThrowsIllegalArgumentWhenTierNameIsTooLong() {
        String projectIdRaw = randomProjectIdString();
        String tooLongTierName = randomAlphanumericOfLength(IsolatedProjects.MAX_ISOLATED_TIER_NAME_LENGTH + 1).toLowerCase(Locale.ROOT);
        expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse(isolatedProjectsEntryJson(projectIdRaw, "{\"index\":\"" + tooLongTierName + "\"}", ""))
        );
    }

    public void testParseThrowsIllegalArgumentWhenProjectFieldMissing() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"tiers\":{}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("missing required field [project] in isolated_projects entry"));
    }

    public void testParseThrowsIllegalArgumentWhenTiersFieldMissing() {
        String projectIdMissingTiersRaw = randomProjectIdString();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"" + projectIdMissingTiersRaw + "\"}]")
        );
        assertThat(
            illegalArgumentException.getMessage(),
            equalTo("missing required field [tiers] for project [" + projectIdMissingTiersRaw + "]")
        );
    }

    public void testParseThrowsIllegalArgumentWhenProjectFieldIsEmptyString() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"\",\"tiers\":{}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("missing required field [project] in isolated_projects entry"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameIsEmptyString() {
        String projectIdRaw = randomProjectIdString();
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"" + projectIdRaw + "\",\"tiers\":{\"index\":\"\"}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("isolated tier name cannot be empty"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameStartsWithHyphen() {
        String projectIdRaw = randomProjectIdString();
        String tierStartingWithHyphen = "-" + randomAlphanumericOfLength(1).toLowerCase(Locale.ROOT);
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse(isolatedProjectsEntryJson(projectIdRaw, "{\"index\":\"" + tierStartingWithHyphen + "\"}", ""))
        );
        assertThat(
            illegalArgumentException.getMessage(),
            equalTo("isolated tier name [" + tierStartingWithHyphen + "] must not start or end with '-'")
        );
    }

    public void testParseThrowsIllegalArgumentWhenTierNameEndsWithHyphen() {
        String projectIdRaw = randomProjectIdString();
        String tierEndingWithHyphen = randomValidIsolatedTierNameExactly(randomIntBetween(1, 5)) + "-";
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse(isolatedProjectsEntryJson(projectIdRaw, "{\"search\":\"" + tierEndingWithHyphen + "\"}", ""))
        );
        assertThat(
            illegalArgumentException.getMessage(),
            equalTo("isolated tier name [" + tierEndingWithHyphen + "] must not start or end with '-'")
        );
    }

    public void testParseThrowsIllegalArgumentWhenTierNameContainsUnderscore() {
        String projectIdRaw = randomProjectIdString();
        String tierWithUnderscore = randomAlphanumericOfLength(randomIntBetween(1, 3)).toLowerCase(Locale.ROOT)
            + "_"
            + randomAlphanumericOfLength(randomIntBetween(1, 3)).toLowerCase(Locale.ROOT);

        expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse(isolatedProjectsEntryJson(projectIdRaw, "{\"index\":\"" + tierWithUnderscore + "\"}", ""))
        );
    }

    public void testParseThrowsParsingExceptionWhenJsonRootIsNotArray() {
        expectThrows(ParsingException.class, () -> IsolatedProjects.parse("{}"));
    }

    public void testParseWrapsIOExceptionCauseInIllegalArgumentWhenInputUnreadableAsJson() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("\0")
        );
        assertTrue(illegalArgumentException.getMessage().startsWith("failed to parse "));
        assertNotNull(illegalArgumentException.getCause());
        assertEquals(IOException.class, illegalArgumentException.getCause().getClass());
    }

    private static String randomProjectIdString() {
        return randomAlphanumericOfLength(randomIntBetween(12, 32));
    }

    /**
     * Isolated tier name matching {@link IsolatedProjects} validation ([a-z0-9-], no leading/trailing '-').
     */
    private static String randomValidIsolatedTierName() {
        return randomValidIsolatedTierNameExactly(randomIntBetween(1, IsolatedProjects.MAX_ISOLATED_TIER_NAME_LENGTH));
    }

    /** Lowercase alphanumeric, satisfying isolated tier naming rules ([a-z0-9]). */
    private static String randomValidIsolatedTierNameExactly(int length) {
        assert length >= 1 && length <= IsolatedProjects.MAX_ISOLATED_TIER_NAME_LENGTH;
        return randomAlphanumericOfLength(length).toLowerCase(Locale.ROOT);
    }

    /** One JSON array entry */
    private static String isolatedProjectsEntryJson(String projectIdRawString, String tiersJsonObjectBody, String suffixAfterEntry) {
        return "[" + "{\"project\":\"" + projectIdRawString + "\",\"tiers\":" + tiersJsonObjectBody + "}" + suffixAfterEntry + "]";
    }
}
