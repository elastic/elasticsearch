/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class ProfileHasPrivilegesRequestTests extends AbstractWireSerializingTestCase<ProfileHasPrivilegesRequest> {

    @Override
    protected Writeable.Reader<ProfileHasPrivilegesRequest> instanceReader() {
        return ProfileHasPrivilegesRequest::new;
    }

    @Override
    protected ProfileHasPrivilegesRequest createTestInstance() {
        List<String> clusterPrivileges = randomSubsetOf(randomIntBetween(1, 5), ClusterPrivilegeResolver.names());
        RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[randomInt(5)];
        for (int i = 0; i < indicesPrivileges.length; i++) {
            indicesPrivileges[i] = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(randomSubsetOf(randomIntBetween(1, 5), IndexPrivilege.names()))
                .indices(randomList(1, 3, () -> randomAlphaOfLengthBetween(2, 8) + (randomBoolean() ? "*" : "")))
                .build();
        }
        RoleDescriptor.ApplicationResourcePrivileges[] appPrivileges = new RoleDescriptor.ApplicationResourcePrivileges[randomInt(5)];
        for (int i = 0; i < appPrivileges.length; i++) {
            appPrivileges[i] = RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application(randomAlphaOfLengthBetween(3, 8))
                .resources(randomList(1, 3, () -> randomAlphaOfLengthBetween(5, 7) + (randomBoolean() ? "*" : "")))
                .privileges(generateRandomStringArray(3, 7, false, false))
                .build();
        }
        return new ProfileHasPrivilegesRequest(
            randomList(5, () -> randomAlphaOfLengthBetween(0, 7)),
            new PrivilegesToCheck(clusterPrivileges.toArray(new String[0]), indicesPrivileges, appPrivileges)
        );
    }

    public void testValidateNullPrivileges() {
        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            randomList(1, 3, () -> randomAlphaOfLengthBetween(0, 5)),
            new PrivilegesToCheck(null, null, null)
        );
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("clusterPrivileges must not be null"));
        assertThat(exception.validationErrors(), hasItem("indexPrivileges must not be null"));
        assertThat(exception.validationErrors(), hasItem("applicationPrivileges must not be null"));
    }

    public void testValidateEmptyPrivileges() {
        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            randomList(1, 3, () -> randomAlphaOfLengthBetween(0, 5)),
            new PrivilegesToCheck(
                new String[0],
                new RoleDescriptor.IndicesPrivileges[0],
                new RoleDescriptor.ApplicationResourcePrivileges[0]
            )
        );
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("must specify at least one privilege"));
    }

    public void testValidateNoWildcardApplicationPrivileges() {
        final ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            randomList(1, 3, () -> randomAlphaOfLengthBetween(0, 5)),
            new PrivilegesToCheck(
                new String[0],
                new RoleDescriptor.IndicesPrivileges[0],
                new RoleDescriptor.ApplicationResourcePrivileges[] {
                    RoleDescriptor.ApplicationResourcePrivileges.builder().privileges("read").application("*").resources("item/1").build() }
            )
        );
        final ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("Application names may not contain '*' (found '*')"));
    }
}
