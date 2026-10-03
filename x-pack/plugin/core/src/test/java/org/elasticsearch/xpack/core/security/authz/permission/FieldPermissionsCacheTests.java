/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FieldPermissionsCacheTests extends ESTestCase {

    public void testFieldPermissionsCaching() {
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        String[] allowed = new String[] { randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*" };
        String[] denied = new String[] {
            allowed[0] + randomAlphaOfLength(5),
            allowed[1] + randomAlphaOfLength(5),
            allowed[2] + randomAlphaOfLength(5) };
        FieldPermissions fieldPermissions = fieldPermissionsCache.getFieldPermissions(allowed, denied);
        assertNotNull(fieldPermissions);
        final String[] allowed2 = randomBoolean() ? allowed : Arrays.copyOf(allowed, allowed.length);
        final String[] denied2 = randomBoolean() ? denied : Arrays.copyOf(denied, denied.length);
        assertSame(fieldPermissions, fieldPermissionsCache.getFieldPermissions(allowed2, denied2));
    }

    public void testMergeFieldPermissions() {
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        String allowedPrefix1 = randomAlphaOfLength(5);
        String allowedPrefix2 = randomAlphaOfLength(5);
        String[] allowed1 = new String[] { allowedPrefix1 + "*" };
        String[] allowed2 = new String[] { allowedPrefix2 + "*" };
        String[] denied1 = new String[] { allowedPrefix1 + "a" };
        String[] denied2 = new String[] { allowedPrefix2 + "a" };
        FieldPermissions fieldPermissions1 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1)
            : new FieldPermissions(fieldPermissionDef(allowed1, denied1));
        FieldPermissions fieldPermissions2 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2)
            : new FieldPermissions(fieldPermissionDef(allowed2, denied2));
        FieldPermissions mergedFieldPermissions = fieldPermissionsCache.union(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(mergedFieldPermissions.grantsAccessTo(allowedPrefix1 + "b"));
        assertTrue(mergedFieldPermissions.grantsAccessTo(allowedPrefix2 + "b"));
        assertFalse(mergedFieldPermissions.grantsAccessTo(denied1[0]));
        assertFalse(mergedFieldPermissions.grantsAccessTo(denied2[0]));

        allowed1 = new String[] { randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*" };
        allowed2 = null;
        denied1 = new String[] { allowed1[0] + "a", allowed1[1] + "a" };
        denied2 = null;
        fieldPermissions1 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1)
            : new FieldPermissions(fieldPermissionDef(allowed1, denied1));
        fieldPermissions2 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2)
            : new FieldPermissions(fieldPermissionDef(allowed2, denied2));
        mergedFieldPermissions = fieldPermissionsCache.union(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertFalse(mergedFieldPermissions.hasFieldLevelSecurity());

        allowed1 = new String[] {};
        allowed2 = new String[] { randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*" };
        denied1 = new String[] {};
        denied2 = new String[] { allowed2[0] + "a", allowed2[1] + "a" };
        fieldPermissions1 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1)
            : new FieldPermissions(fieldPermissionDef(allowed1, denied1));
        fieldPermissions2 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2)
            : new FieldPermissions(fieldPermissionDef(allowed2, denied2));
        mergedFieldPermissions = fieldPermissionsCache.union(Arrays.asList(fieldPermissions1, fieldPermissions2));
        for (String field : allowed2) {
            assertTrue(mergedFieldPermissions.grantsAccessTo(field));
        }
        for (String field : denied2) {
            assertFalse(mergedFieldPermissions.grantsAccessTo(field));
        }

        allowed1 = randomBoolean() ? null : new String[] { "*" };
        allowed2 = randomBoolean() ? null : new String[] { "*" };
        denied1 = new String[] { "a" };
        denied2 = new String[] { "b" };
        fieldPermissions1 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1)
            : new FieldPermissions(fieldPermissionDef(allowed1, denied1));
        fieldPermissions2 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2)
            : new FieldPermissions(fieldPermissionDef(allowed2, denied2));
        mergedFieldPermissions = fieldPermissionsCache.union(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(mergedFieldPermissions.grantsAccessTo("a"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("b"));

        allowed1 = new String[] { "a*" };
        allowed2 = new String[] { "b*" };
        denied1 = new String[] { "aa*" };
        denied2 = null;
        fieldPermissions1 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1)
            : new FieldPermissions(fieldPermissionDef(allowed1, denied1));
        fieldPermissions2 = randomBoolean()
            ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2)
            : new FieldPermissions(fieldPermissionDef(allowed2, denied2));
        mergedFieldPermissions = fieldPermissionsCache.union(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(mergedFieldPermissions.grantsAccessTo("a"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("b"));
        assertFalse(mergedFieldPermissions.grantsAccessTo("aa"));
        assertFalse(mergedFieldPermissions.grantsAccessTo("aa1"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("a1"));
    }

    public void testNonFlsAndFlsMerging() {
        List<FieldPermissions> permissionsList = new ArrayList<>();
        permissionsList.add(new FieldPermissions(fieldPermissionDef(new String[] { "field1" }, null)));
        permissionsList.add(new FieldPermissions(fieldPermissionDef(new String[] { "field2", "query*" }, null)));
        permissionsList.add(new FieldPermissions(fieldPermissionDef(new String[] { "field1", "field2" }, null)));
        permissionsList.add(new FieldPermissions(fieldPermissionDef(new String[] {}, null)));
        permissionsList.add(new FieldPermissions(fieldPermissionDef(null, null)));

        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        for (int i = 0; i < scaledRandomIntBetween(1, 12); i++) {
            Collections.shuffle(permissionsList, random());
            FieldPermissions result = cache.union(permissionsList);
            assertFalse(result.hasFieldLevelSecurity());
        }
    }

    public void testUnionGrantsMetadataFields() {
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        FieldPermissions union = cache.union(
            List.of(
                new FieldPermissions(fieldPermissionDef(new String[] { "f1" }, null)),
                new FieldPermissions(fieldPermissionDef(new String[] { "f2" }, null))
            )
        );
        assertTrue(union.hasFieldLevelSecurity());
        assertTrue(union.grantsAccessTo("f1"));
        assertTrue(union.grantsAccessTo("f2"));
        for (String field : FieldPermissions.METADATA_FIELDS_ALLOWLIST) {
            assertTrue(field, union.grantsAccessTo(field));
            assertTrue(field, union.grantsAccessTo(field + ".sub"));
        }
        assertFalse(union.grantsAccessTo("_some_plugin_meta_field"));
        assertFalse(union.grantsAccessTo("f3"));
    }

    public void testUnionPropagatesLegacyExceptFieldsFlag() {
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        FieldPermissions legacy = new FieldPermissions(fieldPermissionDef(new String[] { "xyz" }, new String[] { "_xyz" }));
        assertTrue(legacy.hasLegacyExceptFields());
        FieldPermissions nonLegacy = new FieldPermissions(fieldPermissionDef(new String[] { "abc*" }, new String[] { "abcd" }));
        assertFalse(nonLegacy.hasLegacyExceptFields());

        FieldPermissions union = cache.union(shuffledList(List.of(legacy, nonLegacy)));
        assertTrue(union.hasLegacyExceptFields());
        assertTrue(union.grantsAccessTo("xyz"));
        assertTrue(union.grantsAccessTo("abc"));
        assertFalse(union.grantsAccessTo("_xyz"));
        assertFalse(union.grantsAccessTo("abcd"));

        FieldPermissions otherNonLegacy = new FieldPermissions(fieldPermissionDef(new String[] { "def*" }, new String[] { "defa" }));
        union = cache.union(shuffledList(List.of(nonLegacy, otherNonLegacy)));
        assertFalse(union.hasLegacyExceptFields());
    }

    public void testValidateFieldPermissionsWithCacheWarnsOnFirstComputationOnly() {
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        MockLog.assertThatLogger(
            () -> cache.validateFieldPermissionsWithCache("role1", fieldPermissionDef(new String[] { "xyz" }, new String[] { "_xyz" })),
            FieldPermissionsCache.class,
            new MockLog.SeenEventExpectation(
                "legacy except fields warning",
                FieldPermissionsCache.class.getName(),
                Level.WARN,
                "Role [role1] has exceptions for field permissions*minimally required metadata fields*"
            )
        );

        // the same definition is cached, so validating it again (even for another role) does not warn a second time
        MockLog.assertThatLogger(
            () -> cache.validateFieldPermissionsWithCache(
                randomFrom("role1", "role2"),
                fieldPermissionDef(new String[] { "xyz" }, new String[] { "_xyz" })
            ),
            FieldPermissionsCache.class,
            new MockLog.UnseenEventExpectation("no repeated warning", FieldPermissionsCache.class.getName(), Level.WARN, "*")
        );
    }

    public void testValidateFieldPermissionsWithCacheDoesNotWarnForValidDefinitions() {
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        MockLog.assertThatLogger(
            () -> cache.validateFieldPermissionsWithCache("role1", fieldPermissionDef(new String[] { "ab*" }, new String[] { "abc" })),
            FieldPermissionsCache.class,
            new MockLog.UnseenEventExpectation("no warning", FieldPermissionsCache.class.getName(), Level.WARN, "*")
        );
    }

    public void testInvalidDefinitionThrows() {
        FieldPermissionsCache cache = new FieldPermissionsCache(Settings.EMPTY);
        FieldPermissionsDefinition invalid = fieldPermissionDef(new String[] { "abc" }, new String[] { randomFrom("xyz", "*", "a*") });
        expectThrows(ElasticsearchSecurityException.class, () -> cache.validateFieldPermissionsWithCache("role1", invalid));
        expectThrows(ElasticsearchSecurityException.class, () -> cache.getFieldPermissions(invalid));
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }
}
