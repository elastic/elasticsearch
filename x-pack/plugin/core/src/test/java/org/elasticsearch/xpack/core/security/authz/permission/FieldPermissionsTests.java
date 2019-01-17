/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class FieldPermissionsTests extends ESTestCase {

    public void testFieldPermissionsIntersection() throws IOException {

        FieldPermissions fieldPermissions1 = new FieldPermissions(
                fieldPermissionDef(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" }));
        FieldPermissions fieldPermissions2 = new FieldPermissions(
                fieldPermissionDef(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" }));

        IndexAccessControl permissions = new IndexAccessControl(true, fieldPermissions1, DocumentPermissions.allowAll());
        IndexAccessControl filteredPermissions = new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll());
        IndexAccessControl indexAccessControl = IndexAccessControl.scopedIndexAccessControl(permissions, filteredPermissions);
        CharacterRunAutomaton automaton = new CharacterRunAutomaton(indexAccessControl.getFieldPermissions().getIncludeAutomaton());
        assertThat(automaton.run("f1"), is(true));
        assertThat(automaton.run("f2"), is(true));
        assertThat(automaton.run("f3"), is(false));
        assertThat(automaton.run("f31"), is(true));
        assertThat(automaton.run("f4"), is(false));

        permissions = new IndexAccessControl(true, fieldPermissions1, DocumentPermissions.allowAll());
        filteredPermissions = new IndexAccessControl(true, fieldPermissions2, DocumentPermissions.allowAll());
        indexAccessControl = IndexAccessControl.scopedIndexAccessControl(permissions, filteredPermissions);
        automaton = new CharacterRunAutomaton(indexAccessControl.getFieldPermissions().getIncludeAutomaton());
        assertThat(automaton.run("f1"), is(true));
        assertThat(automaton.run("f2"), is(false));
        assertThat(automaton.run("f3"), is(false));
        assertThat(automaton.run("f31"), is(true));
        assertThat(automaton.run("f4"), is(false));

        permissions = new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll());
        ;
        filteredPermissions = new IndexAccessControl(true, fieldPermissions2, DocumentPermissions.allowAll());
        indexAccessControl = IndexAccessControl.scopedIndexAccessControl(permissions, filteredPermissions);
        automaton = new CharacterRunAutomaton(indexAccessControl.getFieldPermissions().getIncludeAutomaton());
        assertThat(automaton.run("f1"), is(true));
        assertThat(automaton.run("f2"), is(false));
        assertThat(automaton.run("f3"), is(false));
        assertThat(automaton.run("f31"), is(true));
        assertThat(automaton.run("f4"), is(true));
        assertThat(automaton.run("f5"), is(false));
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }

}
