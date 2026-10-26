/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class FlattenedAutomatonTests extends ESTestCase {

    /** The offsets names are hidden exactly, so in {@code _source} a key below one named {@code offsets} stays visible. */
    public void testExceptKeyIsPartialAccess() {
        var automaton = labelsAutomaton(new String[] { "*" }, new String[] { "labels.secret" });
        assertVisible(
            automaton,
            Set.of("name", "labels.env", "labels.response_time", "labels.offsets.inner"),
            Sets.union(Set.of("labels.secret", "labels.secret.inner"), storageFields("labels"))
        );
    }

    public void testExceptFieldHidesEveryKey() {
        var automaton = labelsAutomaton(new String[] { "*" }, new String[] { "labels" });
        assertVisible(
            automaton,
            Set.of("name", "labelsX"),
            Sets.union(Set.of("labels.env", "labels.secret", "labels.response_time"), storageFields("labels"))
        );
    }

    public void testGrantKeyIsPartialAccess() {
        var automaton = labelsAutomaton(new String[] { "name", "labels.env" }, null);
        assertVisible(
            automaton,
            Set.of("name", "_id", "labels.env", "labels.env.inner"),
            Sets.union(Set.of("labels.secret"), storageFields("labels"))
        );
    }

    public void testGrantFieldGrantsEveryKey() {
        var automaton = labelsAutomaton(new String[] { "name", "labels" }, null);
        assertVisible(
            automaton,
            Sets.union(Set.of("name", "labels.env", "labels.secret", "labels.response_time"), storageFields("labels")),
            Set.of("labelsX", "other")
        );
    }

    /** Every key is visible under {@code labels.*}, so the keyed storage and the keyless root values hold nothing hidden. */
    public void testGrantAllKeysIsFullAccess() {
        for (String[] granted : List.of(new String[] { "name", "labels.*" }, new String[] { "name", "labels", "labels.*" })) {
            var automaton = labelsAutomaton(granted, null);
            assertVisible(automaton, Sets.union(Set.of("name", "labels.env", "labels.secret"), storageFields("labels")), Set.of("other"));
        }
    }

    public void testExceptFieldAndKeysHidesEverything() {
        var automaton = labelsAutomaton(new String[] { "*" }, new String[] { "labels", "labels.*" });
        assertVisible(automaton, Set.of("name"), Sets.union(Set.of("labels.env"), storageFields("labels")));
    }

    /** Inside a flattened field a pattern covers everything below the path it matches, but not a sibling that merely shares a prefix. */
    public void testExceptCoversNestedPaths() {
        var automaton = labelsAutomaton(new String[] { "*" }, new String[] { "labels.user" });
        assertVisible(automaton, Set.of("labels.username"), Set.of("labels.user", "labels.user.name"));
    }

    /** Groups of one definition union: the second group re-grants the key the first group excepts, so every key is visible again. */
    public void testGroupsUnion() {
        var definition = new FieldPermissionsDefinition(
            Set.of(
                new FieldGrantExcludeGroup(new String[] { "*" }, new String[] { "labels.secret" }),
                new FieldGrantExcludeGroup(new String[] { "labels.secret" }, null)
            )
        );
        var automaton = new CharacterRunAutomaton(FlattenedAutomaton.forIndex(List.of(definition), Set.of("labels")));
        assertVisible(automaton, Sets.union(Set.of("name", "labels.env", "labels.secret"), storageFields("labels")), Set.of());
    }

    /** Definitions intersect: an API key limited by its owner's role sees only what both allow. */
    public void testDefinitionsIntersect() {
        FieldPermissions limited = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "labels.secret" })
        ).limitFieldPermissions(new FieldPermissions(new FieldPermissionsDefinition(new String[] { "name", "labels" }, null)));
        var automaton = new CharacterRunAutomaton(FlattenedAutomaton.forIndex(limited.getFieldPermissionsDefinitions(), Set.of("labels")));
        assertVisible(automaton, Set.of("name", "labels.env"), Sets.union(Set.of("labels.secret", "other"), storageFields("labels")));
    }

    /**
     * Outside flattened fields the automaton answers as the role automaton does: a pattern covers the paths below it only inside a
     * flattened field, so the except on {@code secret} does not hide {@code secret.inner}.
     */
    public void testNonFlattenedPathsMatchTheRoleAutomaton() {
        FieldPermissions permissions = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "*" }, new String[] { "secret", "labels.secret" })
        );
        var automaton = new CharacterRunAutomaton(
            FlattenedAutomaton.forIndex(permissions.getFieldPermissionsDefinitions(), Set.of("labels"))
        );
        for (String field : List.of("name", "secret", "secret.inner", "labelsX", randomAlphaOfLength(8))) {
            assertThat(field, automaton.run(field), is(permissions.grantsAccessTo(field)));
        }
    }

    /**
     * The automaton is built over all flattened fields at once, so a mapping at the default total fields limit is the worst case. Half the
     * fields get an except on one key (partial access, storage hidden) and half stay fully visible so both branches run. The build time is
     * logged; it is not asserted because it depends on the machine.
     */
    public void testManyFlattenedFields() {
        int count = 1000;
        List<String> fields = new ArrayList<>(count);
        List<String> excepted = new ArrayList<>(count / 2);
        for (int i = 0; i < count; i++) {
            String field = randomAlphaOfLengthBetween(3, 10) + i;
            fields.add(field);
            if (i % 2 == 0) {
                excepted.add(field + ".secret");
            }
        }
        var definitions = List.of(new FieldPermissionsDefinition(new String[] { "*" }, excepted.toArray(String[]::new)));
        long start = System.nanoTime();
        var automaton = new CharacterRunAutomaton(FlattenedAutomaton.forIndex(definitions, Set.copyOf(fields)));
        logger.info("built the flattened automaton for [{}] flattened fields in [{}] ms", count, (System.nanoTime() - start) / 1_000_000);

        String partial = fields.get(0);
        String full = fields.get(1);
        assertVisible(
            automaton,
            Sets.union(Set.of(partial + ".env", full + ".env", full + ".secret"), storageFields(full)),
            Sets.union(Set.of(partial + ".secret"), storageFields(partial))
        );
    }

    /** The automaton of a single definition applied to an index whose only flattened field is {@code labels}. */
    private static CharacterRunAutomaton labelsAutomaton(String[] granted, String[] denied) {
        return new CharacterRunAutomaton(
            FlattenedAutomaton.forIndex(List.of(new FieldPermissionsDefinition(granted, denied)), Set.of("labels"))
        );
    }

    /**
     * The Lucene field names that hold a flattened field's values: the keyless root and the keyed storage with their companions, and the
     * offsets field that records each array key's name and length, with its companion.
     */
    private static Set<String> storageFields(String field) {
        return Set.of(
            field,
            field + ".counts",
            field + "._keyed",
            field + "._keyed.counts",
            field + "._keyed._ignored",
            field + "._keyed._ignored.counts",
            field + ".offsets",
            field + ".offsets.counts"
        );
    }

    /**
     * One automaton filters Lucene field names and {@code _source} paths alike. Under partial access the flattened field's own name is
     * rejected, which hides its storage; the {@code _source} walk still descends into the object because the state after the name is live.
     */
    private static void assertVisible(CharacterRunAutomaton automaton, Set<String> visible, Set<String> hidden) {
        for (String name : visible) {
            assertThat(name, automaton.run(name), is(true));
        }
        for (String name : hidden) {
            assertThat(name, automaton.run(name), is(false));
        }
    }
}
