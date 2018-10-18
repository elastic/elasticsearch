/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;

import static org.hamcrest.Matchers.equalTo;

public class RunAsPermissionTests extends ESTestCase {

    public void testRunAsPermission_checkUser() {
        final RunAsPermission runAsPermission = new RunAsPermission(new Privilege(randomAlphaOfLength(4), "user*", "runAsUser*"));

        assertThat(runAsPermission.check("user1"), equalTo(true));
        assertThat(runAsPermission.check("runAsUser1"), equalTo(true));
        assertThat(runAsPermission.check("notMatch"), equalTo(false));
    }

    public void testRunAsPermission_isSubsetOf() {
        final RunAsPermission runAsPermissionBase = new RunAsPermission(new Privilege(randomAlphaOfLength(4), "user-*", "runAsUser*"));
        final RunAsPermission runAsPermissionSubsetSuccess = new RunAsPermission(
                new Privilege(randomAlphaOfLength(4), "user-1*", "runAsUser1*"));

        assertThat(runAsPermissionSubsetSuccess.isSubsetOf(runAsPermissionBase), equalTo(true));
        final RunAsPermission runAsPermissionSubsetFail = new RunAsPermission(new Privilege(randomAlphaOfLength(4), "noMatch*"));
        assertThat(runAsPermissionSubsetFail.isSubsetOf(runAsPermissionBase), equalTo(false));
    }

}
