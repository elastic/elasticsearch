/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class InternalUsersTests extends ESTestCase {

    public void testSystemUser() {
        assertThat(InternalUsers.getUser("_system"), is(SystemUser.INSTANCE));
    }

    public void testXPackUser() {
        assertThat(InternalUsers.getUser("_xpack"), is(XPackUser.INSTANCE));
    }

    public void testXPackSecurityUser() {
        assertThat(InternalUsers.getUser("_xpack_security"), is(XPackSecurityUser.INSTANCE));
    }

    public void testSecurityProfileUser() {
        assertThat(InternalUsers.getUser("_security_profile"), is(SecurityProfileUser.INSTANCE));
    }

    public void testAsyncSearchUser() {
        assertThat(InternalUsers.getUser("_async_search"), is(AsyncSearchUser.INSTANCE));
    }

    public void testCrossClusterAccessUser() {
        assertThat(InternalUsers.getUser("_cross_cluster_access"), is(CrossClusterAccessUser.INSTANCE));
    }

    public void testStorageUser() {
        assertThat(InternalUsers.getUser("_storage"), is(StorageInternalUser.INSTANCE));
    }

    public void testRegularUser() {
        var username = randomAlphaOfLengthBetween(4, 12);
        expectThrows(IllegalStateException.class, () -> InternalUsers.getUser(username));
        // Can't test other methods because they have an assert that the provided user is internal
    }

}
