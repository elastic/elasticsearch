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
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.same;

public class FieldPermissionsTests extends ESTestCase {

    public void test() {
        FieldPermissions fieldPermissions = FieldPermissions.DEFAULT;
        fieldPermissions = fieldPermissions.limitFieldPermissions(
            new FieldPermissions(fieldPermissionDef(new String[] { "f1", "f2" }, new String[] { "" }))
        );

        assertThat(fieldPermissions.grantsAccessTo("f1"), is(true));
        assertThat(fieldPermissions.grantsAccessTo("f2"), is(true));
    }

    public void testFieldPermissionsIntersection() {

        final FieldPermissions fieldPermissions = FieldPermissions.DEFAULT;
        final FieldPermissions fieldPermissions1 = new FieldPermissions(
            fieldPermissionDef(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" })
        );
        final FieldPermissions fieldPermissions2 = new FieldPermissions(
            fieldPermissionDef(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" })
        );

        {
            FieldPermissions result = fieldPermissions.limitFieldPermissions(randomFrom(FieldPermissions.DEFAULT, null));
            assertThat(result, is(notNullValue()));
            assertThat(result, IsSame.sameInstance(FieldPermissions.DEFAULT));
        }

        {
            FieldPermissions result = fieldPermissions1.limitFieldPermissions(FieldPermissions.DEFAULT);
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

    public void testMultipleLimiting() {
        // Basic test for a number of permission definitions
        FieldPermissions fieldPermissions = FieldPermissions.DEFAULT;
        final int nSets = randomIntBetween(2, 8);
        final FieldPermissionsDefinition fieldPermissionsDefinition = fieldPermissionDef(
            new String[] { "f1", "f2", "f3*" },
            new String[] { "f3" }
        );
        for (int i = 0; i < nSets; i++) {
            fieldPermissions = fieldPermissions.limitFieldPermissions(new FieldPermissions(fieldPermissionsDefinition));
        }
        final List<FieldPermissionsDefinition> fieldPermissionsDefinitions = fieldPermissions.getFieldPermissionsDefinitions();
        assertNonNullFieldPermissionDefinitions(fieldPermissionsDefinitions, nSets);
        fieldPermissionsDefinitions.forEach(fpd -> assertThat(fpd, equalTo(fieldPermissionsDefinition)));
        assertThat(fieldPermissions.grantsAccessTo(randomFrom("f1", "f2", "f31")), is(true));
        assertThat(fieldPermissions.grantsAccessTo("f3"), is(false));

        // More realistic intersection
        fieldPermissions = FieldPermissions.DEFAULT;
        fieldPermissions = fieldPermissions.limitFieldPermissions(
            new FieldPermissions(fieldPermissionDef(new String[] { "f1", "f2", "f3*", "f4*" }, new String[] { "f3" }))
        );
        fieldPermissions = fieldPermissions.limitFieldPermissions(
            new FieldPermissions(fieldPermissionDef(new String[] { "f2", "f3*", "f4*", "f5*" }, new String[] { "f4" }))
        );
        fieldPermissions = fieldPermissions.limitFieldPermissions(
            new FieldPermissions(fieldPermissionDef(new String[] { "f3*", "f4*", "f5*", "f6" }, new String[] { "f5" }))
        );
        assertNonNullFieldPermissionDefinitions(fieldPermissions.getFieldPermissionsDefinitions(), 3);

        assertThat(fieldPermissions.grantsAccessTo(randomFrom("f1", "f2", "f5", "f6") + randomAlphaOfLengthBetween(0, 10)), is(false));
        assertThat(fieldPermissions.grantsAccessTo("f3"), is(false));
        assertThat(fieldPermissions.grantsAccessTo("f4"), is(false));

        assertThat(fieldPermissions.grantsAccessTo("f3" + randomAlphaOfLengthBetween(1, 10)), is(true));
        assertThat(fieldPermissions.grantsAccessTo("f4" + randomAlphaOfLengthBetween(1, 10)), is(true));
    }

    public void testMustHaveNonNullFieldPermissionsDefinition() {
        final FieldPermissions fieldPermissions0 = FieldPermissions.DEFAULT;
        assertNonNullFieldPermissionDefinitions(fieldPermissions0.getFieldPermissionsDefinitions());
        expectThrows(NullPointerException.class, () -> new FieldPermissions(null));
        expectThrows(NullPointerException.class, () -> new FieldPermissions(null, Automatons.MATCH_ALL));

        final FieldPermissions fieldPermissions03 = randomFrom(
            FieldPermissions.DEFAULT,
            new FieldPermissions(fieldPermissionDef(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" }))
        );
        assertNonNullFieldPermissionDefinitions(fieldPermissions03.limitFieldPermissions(null).getFieldPermissionsDefinitions());
        assertNonNullFieldPermissionDefinitions(
            fieldPermissions03.limitFieldPermissions(FieldPermissions.DEFAULT).getFieldPermissionsDefinitions()
        );
        assertNonNullFieldPermissionDefinitions(
            fieldPermissions03.limitFieldPermissions(
                new FieldPermissions(fieldPermissionDef(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" }))
            ).getFieldPermissionsDefinitions(),
            fieldPermissions03.hasFieldLevelSecurity() ? 2 : 1
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
        final FieldPermissions fieldPermissions3 = FieldPermissions.DEFAULT.limitFieldPermissions(
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

    private void assertNonNullFieldPermissionDefinitions(List<FieldPermissionsDefinition> fieldPermissionsDefinitions) {
        assertNonNullFieldPermissionDefinitions(fieldPermissionsDefinitions, 1);
    }

    private void assertNonNullFieldPermissionDefinitions(List<FieldPermissionsDefinition> fieldPermissionsDefinitions, int expectedSize) {
        assertThat(fieldPermissionsDefinitions, notNullValue());
        assertThat(fieldPermissionsDefinitions, hasSize(expectedSize));
        fieldPermissionsDefinitions.forEach(fieldPermissionsDefinition -> assertThat(fieldPermissionsDefinition, notNullValue()));
    }
}
