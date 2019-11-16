/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.ROLE_CHECKS;

public class RoleDeprecationChecksTests extends ESTestCase {

    public void testCreatePrivilegeCheck() {
        RoleDescriptor.IndicesPrivileges badPriv = RoleDescriptor.IndicesPrivileges.builder()
            .indices(randomAlphaOfLength(10))
            .privileges("create")
            .build();
        RoleDescriptor.IndicesPrivileges fixedPriv = RoleDescriptor.IndicesPrivileges.builder()
            .indices(randomAlphaOfLength(10))
            .privileges("index_doc")
            .build();
        final String badRoleName = randomAlphaOfLength(10);
        RoleDescriptor badRole = new RoleDescriptor(badRoleName, null,
            new RoleDescriptor.IndicesPrivileges[]{badPriv}, null);
        final String fixedRoleName = randomValueOtherThan(badRoleName, () -> randomAlphaOfLength(10));
        RoleDescriptor fixedRole = new RoleDescriptor(fixedRoleName, null,
            new RoleDescriptor.IndicesPrivileges[]{fixedPriv}, null);
        List<RoleDescriptor> roles = Arrays.asList(badRole, fixedRole);

        DeprecationIssue expected =  new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Some roles use deprecated privilege \"create\"",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html", // TODO: Fix this
            "Roles which use the deprevated privilege \"create\": ["
                + badRoleName
                + "]. Please use the privilege \"index_doc\" instead.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(ROLE_CHECKS, c -> c.apply(roles));
        assertEquals(Collections.singletonList(expected), issues);
    }
}
