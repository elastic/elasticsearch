/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.Version;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.FROZEN_BASE_ID_9_5;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.MISSING_FROM_MAIN_ON_9_5;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.check;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.checkAddedIds;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.checkAgainstMain;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.checkOnlyPatchIdsAfterRelease;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.comparedWithMain;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.firstReleaseBaseId;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.format;
import static org.elasticsearch.gradle.internal.release.ValidateIndexVersionsTask.readDefinitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ValidateIndexVersionsTaskTests {

    @Test
    public void testReadDefinitionsIgnoresWrappingAndNonVersionFields() {
        Map<Integer, String> definitions = readDefinitions("""
            public class IndexVersions {
                public static final IndexVersion ZERO = def(0, Version.LATEST);
                public static final IndexVersion A = def(9_094_0_00, Version.LUCENE_10_4_0);
                public static final IndexVersion A_BACKPORT = def(
                    9_094_0_01,
                    Version.LUCENE_10_4_0
                );
                public static final IndexVersion LATEST_DEFINED = A_BACKPORT;
                static final int OTHER = compute(1, 2);
                static boolean java21(Object o) {
                    return o instanceof String s && s.isEmpty() == false;
                }
            }""");
        assertThat(definitions, equalTo(Map.of(0, "ZERO", 9_094_0_00, "A", 9_094_0_01, "A_BACKPORT")));
    }

    @Test
    public void testFirstReleaseBaseId() {
        String csv = "9.4.0,9094000\n9.4.9,9094001\n9.2.0,9039001\n";
        assertThat(firstReleaseBaseId(csv, Version.fromString("9.4.10")), equalTo(9_094_0_00));
        assertThat(firstReleaseBaseId(csv, Version.fromString("9.2.3")), equalTo(9_039_0_00));
        assertThat(firstReleaseBaseId(csv, Version.fromString("9.7.0")), nullValue());
    }

    @Test
    public void testReleasedBranchOnlyAddsPatchIds() {
        int frozen = 9_000_0_00 + ThreadLocalRandom.current().nextInt(1, 200) * 1000;
        int patch = frozen + ThreadLocalRandom.current().nextInt(1, 100);
        assertThat(checkOnlyPatchIdsAfterRelease(List.of(frozen - 1000, frozen, patch), frozen), empty());
        assertThat(checkOnlyPatchIdsAfterRelease(List.of(frozen, frozen + 1000), null), empty());

        // a later base id (the 9_111_0_00 mistake) and a later patch id both claim changes main gated below them
        int later = frozen + ThreadLocalRandom.current().nextInt(1, 5) * 1000;
        List<String> errors = checkOnlyPatchIdsAfterRelease(List.of(frozen, later, later + 1), frozen);
        assertThat("frozen [" + frozen + "] later [" + later + "]", errors, hasSize(2));
        assertThat(errors.get(0), containsString("[" + format(later) + "] is above [" + format(frozen) + "]"));
    }

    @Test
    public void testIdentityWithMain() {
        Map<Integer, String> main = Map.of(9_107_0_00, "A", 9_108_0_00, "B");
        assertThat(checkAgainstMain(Map.of(9_107_0_00, "A"), main, Set.of()), empty());

        // the first 9.5 backport attempt reused main's 9_108_0_00 for a different change
        assertThat(
            checkAgainstMain(Map.of(9_107_0_00, "A", 9_108_0_00, "C"), main, Set.of()),
            contains("Index version [C = 9_108_0_00] is named [B] on main. If main renamed it, backport that change.")
        );
        assertThat(
            checkAgainstMain(Map.of(9_107_0_00, "A", 9_107_0_01, "P"), main, Set.of()),
            contains("Index version [P = 9_107_0_01] is not on main.")
        );

        // main rewrites Lucene versions when Lucene drops old constants, which release branches need not backport
        String lucene = "class IndexVersions { static final IndexVersion A = def(9_107_0_00, %s); }";
        assertThat(
            checkAgainstMain(
                readDefinitions(String.format(Locale.ROOT, lucene, "Version.LUCENE_9_12_1")),
                readDefinitions(String.format(Locale.ROOT, lucene, "parseUnchecked(\"9.12.1\")")),
                Set.of()
            ),
            empty()
        );
    }

    @Test
    public void testPrefixOfMain() {
        int count = ThreadLocalRandom.current().nextInt(3, 50);
        Map<Integer, String> main = new TreeMap<>();
        for (int i = 0; i < count; i++) {
            int id = 9_000_0_00 + i * 1000;
            main.put(id, "V" + i);
        }
        // a branch that skips one of main's base ids below its highest one, like 9.5 skipping 9_108_0_00 to 9_110_0_00
        int skipped = 9_000_0_00 + ThreadLocalRandom.current().nextInt(1, count - 1) * 1000;
        Map<Integer, String> branch = new TreeMap<>(main);
        branch.remove(skipped);

        List<String> errors = checkAgainstMain(branch, main, Set.of());
        assertThat("count [" + count + "] skipped [" + skipped + "]", errors, hasSize(1));
        assertThat(errors.get(0), containsString("[V" + (skipped - 9_000_0_00) / 1000 + " = " + format(skipped) + "] from main is missing"));
        assertThat(checkAgainstMain(branch, main, Set.of(skipped)), empty());

        // main's base ids above the branch's highest base id are not the branch's concern
        branch.remove(9_000_0_00 + (count - 1) * 1000);
        assertThat(checkAgainstMain(branch, main, Set.of(skipped)), empty());
    }

    @Test
    public void testPatchIdsInOrder() {
        int base = 9_000_0_00 + ThreadLocalRandom.current().nextInt(1, 200) * 1000;
        int patches = ThreadLocalRandom.current().nextInt(2, 20);
        Map<Integer, String> main = new TreeMap<>();
        // patch ids of an older branch, which this branch never has
        main.put(base - 1000, "OLD");
        main.put(base - 999, "OLD_PATCH");
        for (int i = 0; i <= patches; i++) {
            main.put(base + i, "P" + i);
        }
        Map<Integer, String> branch = new TreeMap<>(main);
        branch.remove(base - 999);
        assertThat(checkAgainstMain(branch, main, Set.of()), empty());

        // two backports merged out of order: the branch has a later patch id without an earlier one
        int skipped = base + ThreadLocalRandom.current().nextInt(1, patches);
        branch.remove(skipped);
        List<String> errors = checkAgainstMain(branch, main, Set.of());
        assertThat("patches [" + patches + "] skipped [" + skipped + "]", errors, hasSize(1));
        assertThat(errors.get(0), containsString("[P" + (skipped - base) + " = " + format(skipped) + "] from main is missing"));

        // main's patch ids above the branch's highest id are not the branch's concern
        branch.remove(base + patches);
        branch.put(skipped, main.get(skipped));
        assertThat(checkAgainstMain(branch, main, Set.of()), empty());
    }

    @Test
    public void testAddedBaseIds() {
        int base = 9_000_0_00 + ThreadLocalRandom.current().nextInt(2, 200) * 1000;
        int patch = base + ThreadLocalRandom.current().nextInt(1, 99);
        int later = base + ThreadLocalRandom.current().nextInt(1, 5) * 1000;
        // an inherited patch id below the highest base id does not stop the branch from copying a later base id
        assertThat(checkAddedIds(Set.of(base - 999, base, later), Set.of(base - 999, base)), empty());
        assertThat(checkAddedIds(Set.of(base, patch, patch + 1), Set.of(base, patch)), empty());
        // a change may copy a later base id and then add a patch id on top of it
        assertThat(checkAddedIds(Set.of(base, later, later + 1), Set.of(base)), empty());

        // main checks the patch's change with between(patch, next base), so it would treat indices created at the copied base id as lacking it
        String expected = "[" + format(later) + "] cannot follow patch id [" + format(patch) + "]";
        List<String> errors = checkAddedIds(Set.of(base, patch, later), Set.of(base, patch));
        assertThat("patch [" + patch + "] later [" + later + "]", errors, hasSize(1));
        assertThat(errors.get(0), containsString(expected));
        // the same when one change adds both the patch id and the later base id
        errors = checkAddedIds(Set.of(base, patch, later), Set.of(base));
        assertThat("patch [" + patch + "] later [" + later + "]", errors, hasSize(1));
        assertThat(errors.get(0), containsString(expected));
    }

    @Test
    public void testAddedIdsRaiseHighestId() {
        int older = 9_000_0_00 + ThreadLocalRandom.current().nextInt(1, 100) * 1000;
        int base = older + ThreadLocalRandom.current().nextInt(1, 100) * 1000;
        int olderPatch = older + ThreadLocalRandom.current().nextInt(1, 100);
        int patch = base + ThreadLocalRandom.current().nextInt(1, 99);
        Set<Integer> target = Set.of(older, base, patch);
        // backporting only another branch's patch id leaves new indices at the branch's highest id, like 9.6 taking only a 9.5 line
        assertThat(
            checkAddedIds(Set.of(older, olderPatch, base, patch), target),
            contains(containsString("Index versions [" + format(olderPatch) + "] do not raise this branch's highest id [" + format(patch) + "]"))
        );
        // with its own new patch id the branch can tell the change apart, like 9.5 adding 9.4's 9_094_0_01 and its own 9_111_0_01
        assertThat(checkAddedIds(Set.of(older, olderPatch, base, patch, patch + 1), target), empty());
        // replacing a wrong id corrects a mistake, like 9.2 replacing a copied 9_040_0_00 with 9_039_0_03
        assertThat(checkAddedIds(Set.of(older, base, patch), Set.of(older, base, base + 1000)), empty());
        // ids before 8.11 are never the id new indices get, like 9.0 adding V_8_5_3 = 8_05_03_99
        assertThat(checkAddedIds(Set.of(8_05_03_99, older, base, patch), target), empty());
    }

    @Test
    public void testCheckSelectsInputsByBranch() {
        int patches = ThreadLocalRandom.current().nextInt(1, 20);
        List<Integer> main = new ArrayList<>(List.of(9_107_0_00, 9_108_0_00, 9_109_0_00, 9_110_0_00, 9_111_0_00, 9_112_0_00));
        List<Integer> on95 = new ArrayList<>(List.of(9_107_0_00, 9_111_0_00));
        for (int id = 9_111_0_01; id <= 9_111_0_00 + patches; id++) {
            main.add(id);
            on95.add(id);
        }
        String csv = "9.5.0,9107000\n9.6.0,9107000\n";
        Version v95 = Version.fromString("9.5.7");
        List<Integer> on96 = new ArrayList<>(main);
        on96.remove(Integer.valueOf(9_112_0_00));
        Map<String, String> elastic = Map.of("main", source(main), "9.5", source(on95), "9.6", source(on96));

        // 9.5 uses its hard-coded frozen base id and may miss 9_108_0_00 to 9_110_0_00
        assertThat(check(v95, source(on95), csv, "9.5", "patch/serverless-fix", () -> elastic.get("main"), () -> elastic.get("9.5")), empty());
        // any other branch with the same ids gets its frozen base id from the csv and no exceptions
        List<String> errors = check(Version.fromString("9.6.1"), source(on95), csv, "9.6", null, () -> elastic.get("main"), () -> elastic.get("9.6"));
        assertThat("patches [" + patches + "]", errors, hasSize(1 + patches + 3));
        assertThat(errors.get(0), containsString("[9_111_0_00] is above [9_107_0_00]"));
        assertThat(errors.get(1 + patches), containsString("[V9108000 = 9_108_0_00] from main is missing"));

        // a target that is not compared with main reads nothing from git
        assertThat(check(v95, source(on95), csv, "stack/9.5-fix", null, ValidateIndexVersionsTaskTests::unread, ValidateIndexVersionsTaskTests::unread), empty());

        // a 9.5 id between 9_107_0_00 and 9_111_0_00, a skipped base id or a patch id in 9.5.0's range, claims a change for indices main
        // treats as lacking it; this runs in every build, including after merge
        List<Integer> copied = new ArrayList<>(on95);
        int skipped = ThreadLocalRandom.current().nextBoolean()
            ? randomFrom(MISSING_FROM_MAIN_ON_9_5)
            : 9_107_0_00 + ThreadLocalRandom.current().nextInt(1, 100);
        copied.add(skipped);
        assertThat(
            check(v95, source(copied), csv, null, null, ValidateIndexVersionsTaskTests::unread, ValidateIndexVersionsTaskTests::unread),
            contains(containsString("[V" + skipped + " = " + format(skipped) + "] must stay off 9.5"))
        );

        // the target branch, not main, decides which base ids the change adds
        List<Integer> afterPatch = new ArrayList<>(on96);
        afterPatch.add(9_112_0_00);
        errors = check(Version.fromString("9.6.0"), source(afterPatch), "", "9.6", null, () -> elastic.get("main"), () -> elastic.get("9.6"));
        assertThat(errors, contains(containsString("[9_112_0_00] cannot follow patch id [9_111_0_01]")));

        assertThat(
            check(v95, "class IndexVersions {}", csv, "9.5", null, () -> elastic.get("main"), () -> elastic.get("9.5")),
            contains(containsString("Found no index versions"))
        );
    }

    private static String unread() {
        throw new AssertionError("read a branch from git");
    }

    private static int randomFrom(Set<Integer> ids) {
        return List.copyOf(ids).get(ThreadLocalRandom.current().nextInt(ids.size()));
    }

    private static String source(List<Integer> ids) {
        StringBuilder source = new StringBuilder("class IndexVersions {\n");
        for (int id : ids) {
            source.append("    static final IndexVersion V").append(id).append(" = def(").append(id).append(", L);\n");
        }
        return source.append("}").toString();
    }

    @Test
    public void testExceptionsFor95() {
        Map<Integer, String> main = new TreeMap<>();
        int patches = ThreadLocalRandom.current().nextInt(1, 20);
        for (int id = 9_107_0_00; id <= 9_112_0_00; id += 1000) {
            main.put(id, "V" + id);
        }
        for (int id = 9_111_0_01; id <= 9_111_0_00 + patches; id++) {
            main.put(id, "V" + id);
        }
        // 9.5 shipped 9_111_0_00 without 9_108_0_00 to 9_110_0_00, and has not got main's 9_112_0_00
        Map<Integer, String> branch = new TreeMap<>(main);
        branch.keySet().removeAll(MISSING_FROM_MAIN_ON_9_5);
        branch.remove(9_112_0_00);
        assertThat(checkAgainstMain(branch, main, MISSING_FROM_MAIN_ON_9_5), empty());
        List<String> missing = new ArrayList<>();
        for (int id = 9_108_0_00; id <= 9_110_0_00; id += 1000) {
            missing.add("Index version [V" + id + " = " + format(id) + "] from main is missing, but this branch has [" + format(9_111_0_00 + patches) + "].");
        }
        assertThat(checkAgainstMain(branch, main, Set.of()), equalTo(missing));
        assertThat(checkOnlyPatchIdsAfterRelease(branch.keySet(), FROZEN_BASE_ID_9_5), empty());

        // 9.5 must never define an id above 9_111_0_99, which IndexSettings#createdInStateful95 on main relies on
        branch.put(9_112_0_00, main.get(9_112_0_00));
        assertThat(
            checkOnlyPatchIdsAfterRelease(branch.keySet(), FROZEN_BASE_ID_9_5),
            contains(containsString("[9_112_0_00] is above [9_111_0_00]"))
        );
    }

    @Test
    public void testComparedWithMain() {
        String patch = "patch/serverless-fix";
        assertThat(comparedWithMain("9.5", patch), equalTo(true));
        assertThat(comparedWithMain("10.0", null), equalTo(true));
        assertThat(comparedWithMain(patch, patch), equalTo(true));
        assertThat(comparedWithMain(patch, null), equalTo(false));
        // builds that are not PR builds have no target branch
        assertThat(comparedWithMain(null, patch), equalTo(false));
        // main and branches that stacked PRs or features target define ids main does not have yet
        assertThat(comparedWithMain("main", patch), equalTo(false));
        assertThat(comparedWithMain("lucene_snapshot", patch), equalTo(false));
        assertThat(comparedWithMain("feature/esql", patch), equalTo(false));
        assertThat(comparedWithMain("stack/9.5-fix", patch), equalTo(false));
        assertThat(comparedWithMain("8.x", patch), equalTo(false));
    }
}
