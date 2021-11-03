/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.hamcrest.core.IsSame;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.same;

public class FieldPermissionsTests extends ESTestCase {

    public void testFieldPermissionsIntersection() {

        final FieldPermissions fieldPermissions = FieldPermissions.DEFAULT;
        final FieldPermissions fieldPermissions1 = new FieldPermissions(
            fieldPermissionDef(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" })
        );
        final FieldPermissions fieldPermissions2 = new FieldPermissions(
            fieldPermissionDef(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" })
        );

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

    public void testMustHaveNonNullFieldPermissionsDefinition() {
        final FieldPermissions fieldPermissions0 = new FieldPermissions();
        assertThat(fieldPermissions0.getFieldPermissionsDefinition(), notNullValue());
        expectThrows(NullPointerException.class, () -> new FieldPermissions(null));
        expectThrows(NullPointerException.class, () -> new FieldPermissions(null, Automatons.MATCH_ALL));

        final FieldPermissions fieldPermissions03 = randomFrom(
            FieldPermissions.DEFAULT,
            new FieldPermissions(fieldPermissionDef(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" }))
        );
        assertThat(fieldPermissions03.limitFieldPermissions(null).getFieldPermissionsDefinition(), notNullValue());
        assertThat(fieldPermissions03.limitFieldPermissions(FieldPermissions.DEFAULT).getFieldPermissionsDefinition(), notNullValue());
        assertThat(
            fieldPermissions03.limitFieldPermissions(
                new FieldPermissions(fieldPermissionDef(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" }))
            ).getFieldPermissionsDefinition(),
            notNullValue()
        );
    }

    public void testWriteCacheKeyWillDistinguishBetweenDefinitionAndLimitedByDefinition() throws IOException {
        // The overall same grant/except sets but are come from either:
        // 1. Just the definition
        // 2. Just the limited-by definition
        // 3. both
        // The cache key should differentiate between them

        // Just definition
        final BytesStreamOutput out0 = new BytesStreamOutput();
        final FieldPermissions fieldPermissions0 = new FieldPermissions(
            new FieldPermissionsDefinition(
                Set.of(
                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "x*" }, new String[] { "x2" }),
                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "y*" }, new String[] { "y2" }),
                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "z*" }, new String[] { "z2" })
                )
            )
        );
        fieldPermissions0.buildCacheKey(out0, BytesReference::utf8ToString);

        // Mixed definition
        final BytesStreamOutput out1 = new BytesStreamOutput();
        final FieldPermissions fieldPermissions1 = new FieldPermissions(
            new FieldPermissionsDefinition(
                Set.of(
                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "x*" }, new String[] { "x2" }),
                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "y*" }, new String[] { "y2" })
                )
            )
        ).limitFieldPermissions(new FieldPermissions(fieldPermissionDef(new String[] { "z*" }, new String[] { "z2" })));
        fieldPermissions1.buildCacheKey(out1, BytesReference::utf8ToString);

        // Another mixed definition
        final BytesStreamOutput out2 = new BytesStreamOutput();
        final FieldPermissions fieldPermissions2 = new FieldPermissions(
            new FieldPermissionsDefinition(
                Set.of(new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "x*" }, new String[] { "x2" }))
            )
        ).limitFieldPermissions(
            new FieldPermissions(
                new FieldPermissionsDefinition(
                    Set.of(
                        new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "y*" }, new String[] { "y2" }),
                        new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "z*" }, new String[] { "z2" })
                    )
                )
            )
        );
        fieldPermissions2.buildCacheKey(out2, BytesReference::utf8ToString);

        // Just limited by
        final BytesStreamOutput out3 = new BytesStreamOutput();
        final FieldPermissions fieldPermissions3 = new FieldPermissions().limitFieldPermissions(
            new FieldPermissions(
                new FieldPermissionsDefinition(
                    Set.of(
                        new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "x*" }, new String[] { "x2" }),
                        new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "y*" }, new String[] { "y2" }),
                        new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "z*" }, new String[] { "z2" })
                    )
                )
            )
        );
        fieldPermissions3.buildCacheKey(out3, BytesReference::utf8ToString);

        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out1.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out2.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out1.bytes()), BytesReference.toBytes(out2.bytes())), is(false));

        // Just limited by is the same as definition because limitFieldPermissions uses limited-by definition if the original
        // permission is match all
        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out3.bytes())), is(true));
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }

}
