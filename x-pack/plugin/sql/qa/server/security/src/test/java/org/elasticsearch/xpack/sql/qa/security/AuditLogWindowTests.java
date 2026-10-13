/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.sql.qa.security.SqlSecurityTestCase.selectAuditLinesSinceTestStart;
import static org.hamcrest.Matchers.contains;

public class AuditLogWindowTests extends ESTestCase {

    private static final String FIRST = "line-0";

    public void testNoRollReturnsLinesAfterSnapshot() {
        List<String> active = List.of(FIRST, "line-1", "during-test");
        assertThat(select(active, 2, FIRST, Set.of(), Map.of()), contains("during-test"));
    }

    public void testRollUsesArchiveTailThenNewActiveFile() {
        List<String> archived = List.of(FIRST, "line-1", "before-midnight");
        List<String> active = List.of("after-midnight");
        Map<String, List<String>> rolled = Map.of("javaRestTest_audit-2026-09-26-1.json.gz", archived);
        assertThat(select(active, 2, FIRST, Set.of(), rolled), contains("before-midnight", "after-midnight"));
    }

    public void testEmptySnapshotWithNoArchiveIsTheActiveFile() {
        List<String> active = List.of("written-during-test");
        assertThat(select(active, 0, null, Set.of(), Map.of()), contains("written-during-test"));
    }

    public void testEmptySnapshotIncludesANewArchiveInFull() {
        List<String> archived = List.of("before-midnight");
        List<String> active = List.of("after-midnight");
        Map<String, List<String>> rolled = Map.of("javaRestTest_audit-2026-09-26-1.json.gz", archived);
        assertThat(select(active, 0, null, Set.of(), rolled), contains("before-midnight", "after-midnight"));
    }

    public void testJsonAndGzipOfTheSameRollContributeOnce() {
        String plain = "javaRestTest_audit-2026-09-26-1.json";
        Map<String, List<String>> rolled = Map.of(plain, List.of(FIRST, "from-json"), plain + ".gz", List.of(FIRST, "from-gz"));
        assertThat(select(List.of("after"), 1, FIRST, Set.of(), rolled), contains("from-json", "after"));
    }

    public void testGzipOfAnArchiveAlreadyPresentIsNotNew() {
        String plain = "javaRestTest_audit-2026-09-25-1.json";
        Map<String, List<String>> rolled = Map.of(plain + ".gz", List.of("yesterday"));
        List<String> active = List.of("during-test");
        assertThat(select(active, 0, null, Set.of(plain), rolled), contains("during-test"));
    }

    public void testMissingArchiveRetries() {
        expectThrows(AssertionError.class, () -> select(List.of("after-midnight"), 2, FIRST, Set.of(), Map.of()));
    }

    public void testShortArchiveRetries() {
        Map<String, List<String>> rolled = Map.of("javaRestTest_audit-2026-09-26-1.json.gz", List.of(FIRST));
        expectThrows(AssertionError.class, () -> select(List.of("after"), 4, FIRST, Set.of(), rolled));
    }

    private static List<String> select(
        List<String> activeLines,
        int linesBeforeTest,
        String firstLineBeforeTest,
        Set<String> rolledNamesBeforeTest,
        Map<String, List<String>> rolledLinesByName
    ) {
        return selectAuditLinesSinceTestStart(activeLines, linesBeforeTest, firstLineBeforeTest, rolledNamesBeforeTest, rolledLinesByName);
    }
}
