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

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class ProfileHasPrivilegesRequestTests extends AbstractWireSerializingTestCase<ProfileHasPrivilegesRequest> {

    @Override
    protected Writeable.Reader<ProfileHasPrivilegesRequest> instanceReader() {
        return ProfileHasPrivilegesRequest::new;
    }

    @Override
    protected ProfileHasPrivilegesRequest createTestInstance() {
        return new ProfileHasPrivilegesRequest(
            randomList(5, () -> randomAlphaOfLengthBetween(0, 7)),
            randomFrom(randomValidPrivilegesToCheckRequest(), randomInvalidPrivilegesToCheckRequest())
        );
    }

    @Override
    protected ProfileHasPrivilegesRequest mutateInstance(ProfileHasPrivilegesRequest instance) throws IOException {
        if (randomBoolean()) {
            if (instance.profileUids() == null || instance.profileUids().isEmpty()) {
                return new ProfileHasPrivilegesRequest(
                    randomList(1, 3, () -> randomAlphaOfLengthBetween(0, 5)),
                    instance.privilegesToCheck()
                );
            } else {
                return new ProfileHasPrivilegesRequest(
                    randomSubsetOf(randomIntBetween(0, instance.profileUids().size() - 1), instance.profileUids()),
                    instance.privilegesToCheck()
                );
            }
        } else {
            return new ProfileHasPrivilegesRequest(instance.profileUids(), newMutatePrivileges(instance.privilegesToCheck()));
        }
    }

    public void testValidateNullPrivileges() {
        ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(
            randomList(1, 3, () -> randomAlphaOfLengthBetween(0, 5)),
            new PrivilegesToCheck(null, null, null)
        );
        ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("clusterPrivileges must not be null"));
        assertThat(exception.validationErrors(), hasItem("indexPrivileges must not be null"));
        assertThat(exception.validationErrors(), hasItem("applicationPrivileges must not be null"));
        request = new ProfileHasPrivilegesRequest(randomList(1, 3, () -> randomAlphaOfLengthBetween(0, 5)), null);
        exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), hasItem("privileges to check must not be null"));
    }

    public void testValidateNullOrEmptyProfileUids() {
        ProfileHasPrivilegesRequest request = new ProfileHasPrivilegesRequest(null, randomValidPrivilegesToCheckRequest());
        ActionRequestValidationException exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), contains("profile uids must not be null"));
        request = new ProfileHasPrivilegesRequest(List.of(), randomValidPrivilegesToCheckRequest());
        exception = request.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), contains("profile uids list must not be empty"));
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

    public static PrivilegesToCheck randomValidPrivilegesToCheckRequest() {
        String[] clusterPrivileges = randomClusterPrivileges();
        RoleDescriptor.IndicesPrivileges[] indicesPrivileges = randomIndicesPrivileges();
        RoleDescriptor.ApplicationResourcePrivileges[] appPrivileges = randomApplicationResourcePrivileges();
        return new PrivilegesToCheck(clusterPrivileges, indicesPrivileges, appPrivileges);
    }

    private static String[] randomClusterPrivileges() {
        return randomSubsetOf(randomIntBetween(1, 5), ClusterPrivilegeResolver.names()).toArray(new String[0]);
    }

    private static RoleDescriptor.IndicesPrivileges[] randomIndicesPrivileges() {
        RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[randomIntBetween(1, 5)];
        for (int i = 0; i < indicesPrivileges.length; i++) {
            indicesPrivileges[i] = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(randomSubsetOf(randomIntBetween(1, 5), IndexPrivilege.names()))
                .indices(randomList(1, 3, () -> randomAlphaOfLengthBetween(2, 8) + (randomBoolean() ? "*" : "")))
                .build();
        }
        return indicesPrivileges;
    }

    private static RoleDescriptor.ApplicationResourcePrivileges[] randomApplicationResourcePrivileges() {
        RoleDescriptor.ApplicationResourcePrivileges[] appPrivileges = new RoleDescriptor.ApplicationResourcePrivileges[randomIntBetween(
            1,
            5
        )];
        for (int i = 0; i < appPrivileges.length; i++) {
            appPrivileges[i] = RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application(randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 8))
                .resources(randomList(1, 3, () -> randomAlphaOfLengthBetween(5, 7) + (randomBoolean() ? "*" : "")))
                .privileges(generateRandomStringArray(3, 7, false, false))
                .build();
        }
        return appPrivileges;
    }

    private PrivilegesToCheck randomInvalidPrivilegesToCheckRequest() {
        return randomFrom(
            new PrivilegesToCheck(randomBoolean() ? null : new String[0], randomIndicesPrivileges(), randomApplicationResourcePrivileges()),
            new PrivilegesToCheck(
                randomClusterPrivileges(),
                randomBoolean() ? null : new RoleDescriptor.IndicesPrivileges[0],
                randomApplicationResourcePrivileges()
            ),
            new PrivilegesToCheck(
                randomClusterPrivileges(),
                randomIndicesPrivileges(),
                randomBoolean() ? null : new RoleDescriptor.ApplicationResourcePrivileges[0]
            )
        );
    }

    private PrivilegesToCheck newMutatePrivileges(PrivilegesToCheck toMutate) {
        final int choice = randomIntBetween(1, 3);
        switch (choice) {
            case 1 -> {
                if (toMutate.cluster() == null || toMutate.cluster().length == 0) {
                    return new PrivilegesToCheck(randomClusterPrivileges(), toMutate.index(), toMutate.application());
                } else {
                    return new PrivilegesToCheck(
                        randomSubsetOf(randomIntBetween(0, toMutate.cluster().length - 1), toMutate.cluster()).toArray(new String[0]),
                        toMutate.index(),
                        toMutate.application()
                    );
                }
            }
            case 2 -> {
                if (toMutate.index() == null || toMutate.index().length == 0) {
                    return new PrivilegesToCheck(toMutate.cluster(), randomIndicesPrivileges(), toMutate.application());
                } else {
                    return new PrivilegesToCheck(
                        toMutate.cluster(),
                        randomSubsetOf(randomIntBetween(0, toMutate.index().length - 1), toMutate.index()).toArray(
                            new RoleDescriptor.IndicesPrivileges[0]
                        ),
                        toMutate.application()
                    );
                }
            }
            default -> {
                if (toMutate.application() == null || toMutate.application().length == 0) {
                    return new PrivilegesToCheck(toMutate.cluster(), toMutate.index(), randomApplicationResourcePrivileges());
                } else {
                    return new PrivilegesToCheck(
                        toMutate.cluster(),
                        toMutate.index(),
                        randomSubsetOf(randomIntBetween(0, toMutate.application().length - 1), toMutate.application()).toArray(
                            new RoleDescriptor.ApplicationResourcePrivileges[0]
                        )
                    );
                }
            }
        }
    }
}
