/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class AutomatonFieldPredicateTests extends ESTestCase {
    public void testMatching() {
        String str = randomAlphaOfLength(10);
        FieldPredicate pred = predicate(new String[] { str }, null);
        assertTrue(pred.test(str));
        assertFalse(pred.test(str + randomAlphaOfLength(1)));
    }

    public void testHash() {
        FieldPredicate predA = predicate(new String[] { "a" }, null);
        FieldPredicate predB = predicate(new String[] { "b" }, null);

        assertThat(predA.modifyHash("a"), not(equalTo(predB.modifyHash("a"))));
    }

    /**
     * Two definitions can produce the same merged automaton, and so the same role hash, yet differ on a flattened field: within one group
     * except wins, so the first definition hides every key of {@code labels} while the second grants {@code labels.env}. field_caps caches
     * by the hash of the predicate it lists with, so the derived predicates must hash differently.
     */
    public void testHashDistinguishesDefinitionsWithEqualAutomata() {
        FieldPermissions a = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "labels", "labels.env" }, new String[] { "labels" })
        );
        FieldPermissions b = new FieldPermissions(new FieldPermissionsDefinition(new String[] { "labels.env" }, null));
        assertThat(Automatons.subsetOf(a.getIncludeAutomaton(), b.getIncludeAutomaton()), is(true));
        assertThat(Automatons.subsetOf(b.getIncludeAutomaton(), a.getIncludeAutomaton()), is(true));
        assertThat(a.fieldPredicate().modifyHash("h"), equalTo(b.fieldPredicate().modifyHash("h")));

        FieldPredicate flattenedA = a.fieldPredicate().withFlattenedFields(Set.of("labels"));
        FieldPredicate flattenedB = b.fieldPredicate().withFlattenedFields(Set.of("labels"));
        assertThat(flattenedA.modifyHash("h"), not(equalTo(flattenedB.modifyHash("h"))));
        assertThat(flattenedA.test("labels"), is(false));
        assertThat(flattenedB.test("labels"), is(true));
    }

    public void testWithoutFlattenedFieldsReturnsTheRolePredicate() {
        FieldPredicate pred = predicate(new String[] { randomAlphaOfLength(5) }, null);
        assertThat(pred.withFlattenedFields(Set.of()), sameInstance(pred));
    }

    public void testAndDerivesBothSides() {
        String root = randomAlphaOfLength(5);
        FieldPredicate pred = new FieldPredicate.And(predicate(new String[] { root + ".env" }, null), FieldPredicate.ACCEPT_ALL);
        assertThat(pred.test(root), is(false));
        assertThat(pred.withFlattenedFields(Set.of(root)).test(root), is(true));
    }

    private static FieldPredicate predicate(String[] granted, String[] denied) {
        return new FieldPermissions(new FieldPermissionsDefinition(granted, denied)).fieldPredicate();
    }
}
