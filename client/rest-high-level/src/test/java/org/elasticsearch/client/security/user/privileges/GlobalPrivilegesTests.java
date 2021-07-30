/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GlobalPrivilegesTests extends AbstractXContentTestCase<GlobalPrivileges> {

    private static long idCounter = 0;

    public static GlobalPrivileges buildRandomManageApplicationPrivilege() {
        final Map<String, Object> privilege = new HashMap<>();
        privilege.put("applications", Arrays.asList(generateRandomStringArray(4, 4, false)));
        final GlobalOperationPrivilege priv = new GlobalOperationPrivilege("application", "manage", privilege);
        return new GlobalPrivileges(Arrays.asList(priv));
    }

    public static GlobalOperationPrivilege buildRandomGlobalScopedPrivilege() {
        final Map<String, Object> privilege = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            if (randomBoolean()) {
                privilege.put(randomAlphaOfLength(2) + idCounter++, randomAlphaOfLengthBetween(0, 4));
            } else {
                privilege.put(randomAlphaOfLength(2) + idCounter++, Arrays.asList(generateRandomStringArray(4, 4, false)));
            }
        }
        return new GlobalOperationPrivilege(randomFrom(GlobalPrivileges.CATEGORIES), randomAlphaOfLength(2) + idCounter++, privilege);
    }

    @Override
    protected GlobalPrivileges createTestInstance() {
        final List<GlobalOperationPrivilege> privilegeList = Arrays
                .asList(randomArray(1, 4, size -> new GlobalOperationPrivilege[size], () -> buildRandomGlobalScopedPrivilege()));
        return new GlobalPrivileges(privilegeList);
    }

    @Override
    protected GlobalPrivileges doParseInstance(XContentParser parser) throws IOException {
        return GlobalPrivileges.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false; // true really means inserting bogus privileges
    }

    public void testEmptyOrNullGlobalOperationPrivilege() {
        final Map<String, Object> privilege = randomBoolean() ? null : Collections.emptyMap();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GlobalOperationPrivilege(randomAlphaOfLength(2), randomAlphaOfLength(2), privilege));
        assertThat(e.getMessage(), is("privileges cannot be empty or null"));
    }

    public void testEmptyOrNullGlobalPrivileges() {
        final List<GlobalOperationPrivilege> privileges = randomBoolean() ? null : Collections.emptyList();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobalPrivileges(privileges));
        assertThat(e.getMessage(), is("Privileges cannot be empty or null"));
    }

    public void testDuplicateGlobalOperationPrivilege() {
        final GlobalOperationPrivilege privilege = buildRandomGlobalScopedPrivilege();
        // duplicate
        final GlobalOperationPrivilege privilege2 = new GlobalOperationPrivilege(privilege.getCategory(), privilege.getOperation(),
                new HashMap<>(privilege.getRaw()));
        final GlobalPrivileges globalPrivilege = new GlobalPrivileges(Arrays.asList(privilege, privilege2));
        assertThat(globalPrivilege.getPrivileges().size(), is(1));
        assertThat(globalPrivilege.getPrivileges().iterator().next(), is(privilege));
    }

    public void testSameScopeGlobalOperationPrivilege() {
        final GlobalOperationPrivilege privilege = buildRandomGlobalScopedPrivilege();
        final GlobalOperationPrivilege sameOperationPrivilege = new GlobalOperationPrivilege(privilege.getCategory(),
                privilege.getOperation(), buildRandomGlobalScopedPrivilege().getRaw());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GlobalPrivileges(Arrays.asList(privilege, sameOperationPrivilege)));
        assertThat(e.getMessage(), is("Different privileges for the same category and operation are not permitted"));
    }

    public void testEqualsHashCode() {
        final List<GlobalOperationPrivilege> privilegeList = Arrays
                .asList(randomArray(1, 4, size -> new GlobalOperationPrivilege[size], () -> buildRandomGlobalScopedPrivilege()));
        GlobalPrivileges globalPrivileges = new GlobalPrivileges(privilegeList);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(globalPrivileges, (original) -> {
            return new GlobalPrivileges(original.getPrivileges());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(globalPrivileges, (original) -> {
            return new GlobalPrivileges(original.getPrivileges());
        }, (original) -> {
            final List<GlobalOperationPrivilege> newList = Arrays
                    .asList(randomArray(1, 4, size -> new GlobalOperationPrivilege[size], () -> buildRandomGlobalScopedPrivilege()));
            return new GlobalPrivileges(newList);
        });
    }
}
