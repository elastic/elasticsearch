/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.is;

public class InternalUsersTests extends ESTestCase {

    public void testSystemUser() {
        assertThat(InternalUsers.getUser("_system"), is(SystemUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(SystemUser.INSTANCE), is("_system"));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InternalUsers.getRoleDescriptor(SystemUser.INSTANCE)
        );
        assertThat(e, throwableWithMessage("should never try to get the roles for internal user [_system]"));
    }

    public void testXPackUser() {
        assertThat(InternalUsers.getUser("_xpack"), is(XPackUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(XPackUser.INSTANCE), is("_xpack"));
        assertThat(InternalUsers.getRoleDescriptor(XPackUser.INSTANCE), is(XPackUser.ROLE_DESCRIPTOR));
    }

    public void testXPackSecurityUser() {
        assertThat(InternalUsers.getUser("_xpack_security"), is(XPackSecurityUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(XPackSecurityUser.INSTANCE), is("_xpack_security"));
        assertThat(InternalUsers.getRoleDescriptor(XPackSecurityUser.INSTANCE), is(XPackSecurityUser.ROLE_DESCRIPTOR));
    }

    public void testSecurityProfileUser() {
        assertThat(InternalUsers.getUser("_security_profile"), is(SecurityProfileUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(SecurityProfileUser.INSTANCE), is("_security_profile"));
        assertThat(InternalUsers.getRoleDescriptor(SecurityProfileUser.INSTANCE), is(SecurityProfileUser.ROLE_DESCRIPTOR));
    }

    public void testAsyncSearchUser() {
        assertThat(InternalUsers.getUser("_async_search"), is(AsyncSearchUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(AsyncSearchUser.INSTANCE), is("_async_search"));
        assertThat(InternalUsers.getRoleDescriptor(AsyncSearchUser.INSTANCE), is(AsyncSearchUser.ROLE_DESCRIPTOR));
    }

    public void testCrossClusterAccessUser() {
        assertThat(InternalUsers.getUser("_cross_cluster_access"), is(CrossClusterAccessUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(CrossClusterAccessUser.INSTANCE), is("_cross_cluster_access"));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InternalUsers.getRoleDescriptor(CrossClusterAccessUser.INSTANCE)
        );
        assertThat(e, throwableWithMessage("should never try to get the roles for internal user [_cross_cluster_access]"));
    }

    public void testStorageUser() {
        assertThat(InternalUsers.getUser("_storage"), is(StorageInternalUser.INSTANCE));
        assertThat(InternalUsers.getInternalUserName(StorageInternalUser.INSTANCE), is("_storage"));
        assertThat(InternalUsers.getRoleDescriptor(StorageInternalUser.INSTANCE), is(StorageInternalUser.ROLE_DESCRIPTOR));
    }

    public void testRegularUser() {
        var username = randomAlphaOfLengthBetween(4, 12);
        expectThrows(IllegalStateException.class, () -> InternalUsers.getUser(username));
        // Can't test other methods because they have an assert that the provided user is internal
    }

}
