/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.core.IsSame;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.same;

public class FieldPermissionsTests extends ESTestCase {

    public void testFieldPermissionsIntersection() throws IOException {

        final FieldPermissions fieldPermissions = FieldPermissions.DEFAULT;
        final FieldPermissions fieldPermissions1 = new FieldPermissions(
                fieldPermissionDef(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" }));
        final FieldPermissions fieldPermissions2 = new FieldPermissions(
                fieldPermissionDef(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" }));

        {
            FieldPermissions result = fieldPermissions.limitFieldPermissions(randomFrom(new FieldPermissions(), null));
            assertThat(result, is(notNullValue()));
            assertThat(result, IsSame.sameInstance(FieldPermissions.DEFAULT));
        }

        {
            FieldPermissions result = fieldPermissions1.limitFieldPermissions(new FieldPermissions());
            assertThat(result, is(notNullValue()));
            assertThat(result, not(same(fieldPermissions)));
            assertThat(result, not(same(fieldPermissions1)));
            CharacterRunAutomaton automaton = new CharacterRunAutomaton(result.getIncludeAutomaton());
            assertThat(automaton.run("f1"), is(true));
            assertThat(automaton.run("f2"), is(true));
            assertThat(automaton.run("f3"), is(false));
            assertThat(automaton.run("f31"), is(true));
            assertThat(automaton.run("f4"), is(false));
        }

        {
            FieldPermissions result = fieldPermissions1.limitFieldPermissions(fieldPermissions2);
            assertThat(result, is(notNullValue()));
            assertThat(result, not(same(fieldPermissions1)));
            assertThat(result, not(same(fieldPermissions2)));
            CharacterRunAutomaton automaton = new CharacterRunAutomaton(result.getIncludeAutomaton());
            assertThat(automaton.run("f1"), is(true));
            assertThat(automaton.run("f2"), is(false));
            assertThat(automaton.run("f3"), is(false));
            assertThat(automaton.run("f31"), is(true));
            assertThat(automaton.run("f4"), is(false));
        }

        {
            FieldPermissions result = fieldPermissions.limitFieldPermissions(fieldPermissions2);
            assertThat(result, is(notNullValue()));
            assertThat(result, not(same(fieldPermissions1)));
            assertThat(result, not(same(fieldPermissions2)));
            CharacterRunAutomaton automaton = new CharacterRunAutomaton(result.getIncludeAutomaton());
            assertThat(automaton.run("f1"), is(true));
            assertThat(automaton.run("f2"), is(false));
            assertThat(automaton.run("f3"), is(false));
            assertThat(automaton.run("f31"), is(true));
            assertThat(automaton.run("f4"), is(true));
            assertThat(automaton.run("f5"), is(false));
        }
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }

}
