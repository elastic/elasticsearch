/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class FieldPermissionsCacheTests extends ESTestCase {

    public void testFieldPermissionsCaching() {
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        String[] allowed = new String[]{randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*"};
        String[] denied = new String[]{allowed[0] + randomAsciiOfLength(5), allowed[1] + randomAsciiOfLength(5),
                allowed[2] + randomAsciiOfLength(5)};
        FieldPermissions fieldPermissions = fieldPermissionsCache.getFieldPermissions(allowed, denied);
        assertNotNull(fieldPermissions);
        final String[] allowed2 = randomBoolean() ? allowed : Arrays.copyOf(allowed, allowed.length);
        final String[] denied2 = randomBoolean() ? denied : Arrays.copyOf(denied, denied.length);
        assertSame(fieldPermissions, fieldPermissionsCache.getFieldPermissions(allowed2, denied2));
    }

    public void testMergeFieldPermissions() {
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        String allowedPrefix1 = randomAsciiOfLength(5);
        String allowedPrefix2 = randomAsciiOfLength(5);
        String[] allowed1 = new String[]{allowedPrefix1 + "*"};
        String[] allowed2 = new String[]{allowedPrefix2 + "*"};
        String[] denied1 = new String[]{allowedPrefix1 + "a"};
        String[] denied2 = new String[]{allowedPrefix2 + "a"};
        FieldPermissions fieldPermissions1 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1) :
                new FieldPermissions(allowed1, denied1);
        FieldPermissions fieldPermissions2 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2) :
                new FieldPermissions(allowed2, denied2);
        FieldPermissions mergedFieldPermissions =
                fieldPermissionsCache.getFieldPermissions(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(mergedFieldPermissions.grantsAccessTo(allowedPrefix1 + "b"));
        assertTrue(mergedFieldPermissions.grantsAccessTo(allowedPrefix2 + "b"));
        assertFalse(mergedFieldPermissions.grantsAccessTo(denied1[0]));
        assertFalse(mergedFieldPermissions.grantsAccessTo(denied2[0]));

        allowed1 = new String[]{randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*"};
        allowed2 = null;
        denied1 = new String[]{allowed1[0] + "a", allowed1[1] + "a"};
        denied2 = null;
        fieldPermissions1 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1) :
                new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2) :
                new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions =
                fieldPermissionsCache.getFieldPermissions(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertFalse(mergedFieldPermissions.hasFieldLevelSecurity());

        allowed1 = new String[]{};
        allowed2 = new String[]{randomAsciiOfLength(5) + "*", randomAsciiOfLength(5) + "*"};
        denied1 = new String[]{};
        denied2 = new String[]{allowed2[0] + "a", allowed2[1] + "a"};
        fieldPermissions1 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1) :
                new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2) :
                new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions =
                fieldPermissionsCache.getFieldPermissions(Arrays.asList(fieldPermissions1, fieldPermissions2));
        for (String field : allowed2) {
            assertTrue(mergedFieldPermissions.grantsAccessTo(field));
        }
        for (String field : denied2) {
            assertFalse(mergedFieldPermissions.grantsAccessTo(field));
        }

        allowed1 = randomBoolean() ? null : new String[]{"*"};
        allowed2 = randomBoolean() ? null : new String[]{"*"};
        denied1 = new String[]{"a"};
        denied2 = new String[]{"b"};
        fieldPermissions1 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1) :
                new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2) :
                new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions =
                fieldPermissionsCache.getFieldPermissions(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(mergedFieldPermissions.grantsAccessTo("a"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("b"));

        // test merge does not remove _all
        allowed1 = new String[]{"_all"};
        allowed2 = new String[]{};
        denied1 = null;
        denied2 = null;
        fieldPermissions1 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1) :
                new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2) :
                new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions =
                fieldPermissionsCache.getFieldPermissions(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(fieldPermissions1.grantsAccessTo("_all"));
        assertFalse(fieldPermissions2.grantsAccessTo("_all"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("_all"));

        allowed1 = new String[] { "a*" };
        allowed2 = new String[] { "b*" };
        denied1 = new String[] { "aa*" };
        denied2 = null;
        fieldPermissions1 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed1, denied1) :
                new FieldPermissions(allowed1, denied1);
        fieldPermissions2 = randomBoolean() ? fieldPermissionsCache.getFieldPermissions(allowed2, denied2) :
                new FieldPermissions(allowed2, denied2);
        mergedFieldPermissions =
                fieldPermissionsCache.getFieldPermissions(Arrays.asList(fieldPermissions1, fieldPermissions2));
        assertTrue(mergedFieldPermissions.grantsAccessTo("a"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("b"));
        assertFalse(mergedFieldPermissions.grantsAccessTo("aa"));
        assertFalse(mergedFieldPermissions.grantsAccessTo("aa1"));
        assertTrue(mergedFieldPermissions.grantsAccessTo("a1"));
    }
}
