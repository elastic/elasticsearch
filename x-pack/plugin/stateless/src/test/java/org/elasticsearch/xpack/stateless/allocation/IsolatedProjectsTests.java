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
        IsolatedProjects isolatedProjects = IsolatedProjects.parse("[{\"project\":\"pz\",\"tiers\":{}}]");
        ProjectId projectId = ProjectId.fromId("pz");
        assertFalse(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).isPresent());
        assertFalse(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).isPresent());
    }

    public void testIsolatedTierNameReturnsEmptyWhenProjectAbsentFromParsedMap() {
        IsolatedProjects isolatedProjects = IsolatedProjects.parse("[{\"project\":\"knownp\",\"tiers\":{\"index\":\"pool\"}}]");
        ProjectId unknownProjectId = ProjectId.fromId("otherpr");
        assertFalse(isolatedProjects.isolatedTierName(unknownProjectId, IsolationShardTier.INDEX).isPresent());
        assertFalse(isolatedProjects.isolatedTierName(unknownProjectId, IsolationShardTier.SEARCH).isPresent());
    }

    public void testParseThenIsolatedTierNameReturnsPoolsForIndexAndSearch() {
        IsolatedProjects isolatedProjects = IsolatedProjects.parse(
            "[{\"project\":\"proj1abcd\",\"tiers\":{\"index\":\"iso-n1\",\"search\":\"iso-s1\"}}]"
        );
        ProjectId projectId = ProjectId.fromId("proj1abcd");
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).orElse(null), equalTo("iso-n1"));
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).orElse(null), equalTo("iso-s1"));
    }

    public void testParseMergesTierIsolationWhenSameProjectAppearsMultipleTimes() {
        IsolatedProjects isolatedProjects = IsolatedProjects.parse("""
            [
              {"project":"p1","tiers":{"index":"first"}},
              {"project":"p1","tiers":{"search":"second","index":"override"}}
            ]""");
        ProjectId projectId = ProjectId.fromId("p1");
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.INDEX).orElse(null), equalTo("override"));
        assertThat(isolatedProjects.isolatedTierName(projectId, IsolationShardTier.SEARCH).orElse(null), equalTo("second"));
    }

    public void testParseAllowsTierNameAtMaximumLength() {
        String isolatedTierNameAtMaxLength = "abcdefghijklmno";
        IsolatedProjects isolatedProjects = IsolatedProjects.parse(
            String.format(
                Locale.ROOT,
                "[{\"project\":\"plen\",\"tiers\":{\"index\":\"%s\",\"search\":\"%s\"}}]",
                isolatedTierNameAtMaxLength,
                isolatedTierNameAtMaxLength
            )
        );
        ProjectId projectId = ProjectId.fromId("plen");
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
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{},\"x\":1}]")
        );
        assertThat(illegalArgumentException.getMessage().contains("unknown field"), equalTo(true));
    }

    public void testParseThrowsIllegalArgumentWhenTiersObjectHasUnknownField() {
        expectThrows(IllegalArgumentException.class, () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"ml\":\"x\"}}]"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameHasIllegalCharactersOrLength() {
        expectThrows(IllegalArgumentException.class, () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"index\":\"BAD\"}}]"));
        expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"index\":\"name-longer-than-15\"}}]")
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
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"p1only\"}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("missing required field [tiers] for project [p1only]"));
    }

    public void testParseThrowsIllegalArgumentWhenProjectFieldIsEmptyString() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"\",\"tiers\":{}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("missing required field [project] in isolated_projects entry"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameIsEmptyString() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"index\":\"\"}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("isolated tier name cannot be empty"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameStartsWithHyphen() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"index\":\"-x\"}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("isolated tier name [-x] must not start or end with '-'"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameEndsWithHyphen() {
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"search\":\"ab-\"}}]")
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("isolated tier name [ab-] must not start or end with '-'"));
    }

    public void testParseThrowsIllegalArgumentWhenTierNameContainsUnderscore() {
        expectThrows(IllegalArgumentException.class, () -> IsolatedProjects.parse("[{\"project\":\"p1\",\"tiers\":{\"index\":\"a_b\"}}]"));
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
}
