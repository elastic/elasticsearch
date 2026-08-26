/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DeleteUserManagedServiceAccountRequestTests extends AbstractWireSerializingTestCase<DeleteUserManagedServiceAccountRequest> {

    @Override
    protected Writeable.Reader<DeleteUserManagedServiceAccountRequest> instanceReader() {
        return DeleteUserManagedServiceAccountRequest::new;
    }

    @Override
    protected DeleteUserManagedServiceAccountRequest createTestInstance() {
        return newRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomFrom(WriteRequest.RefreshPolicy.values()),
            randomBoolean()
        );
    }

    @Override
    protected DeleteUserManagedServiceAccountRequest mutateInstance(DeleteUserManagedServiceAccountRequest instance) {
        return switch (between(0, 3)) {
            case 0 -> newRequest(
                randomValueOtherThan(instance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.getServiceName(),
                instance.getRefreshPolicy(),
                instance.isForce()
            );
            case 1 -> newRequest(
                instance.getNamespace(),
                randomValueOtherThan(instance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.getRefreshPolicy(),
                instance.isForce()
            );
            case 2 -> newRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                randomValueOtherThan(instance.getRefreshPolicy(), () -> randomFrom(WriteRequest.RefreshPolicy.values())),
                instance.isForce()
            );
            case 3 -> newRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                instance.getRefreshPolicy(),
                instance.isForce() == false
            );
            default -> throw new AssertionError("between(0, 3) returned something outside its own bounds");
        };
    }

    public void testDeleteIsNotForcedByDefault() {
        assertThat(new DeleteUserManagedServiceAccountRequest("my-team", "worker").isForce(), is(false));
    }

    public void testAnAccountThatCouldExistIsAccepted() {
        assertThat(new DeleteUserManagedServiceAccountRequest("my-team", "worker").validate(), nullValue());
    }

    /**
     * Deleting is not gated on the account existing, but a name no user-managed account could carry is still refused:
     * answering "not found" would hide that the name is the problem.
     */
    public void testTheReservedNamespaceIsRejectedInAnyCase() {
        for (String namespace : new String[] { "elastic", "ELASTIC", "Elastic" }) {
            final ActionRequestValidationException e = new DeleteUserManagedServiceAccountRequest(namespace, "fleet-server").validate();
            assertThat("namespace [" + namespace + "] should be reserved", e, notNullValue());
            assertThat(e.validationErrors(), contains("the [elastic] namespace is reserved for built-in service accounts"));
        }
    }

    public void testEveryProblemWithTheAccountNameIsReportedAtOnce() {
        final ActionRequestValidationException e = new DeleteUserManagedServiceAccountRequest("my*team", "worker*").validate();
        assertThat(e, notNullValue());
        assertThat(
            e.validationErrors(),
            contains(
                containsString("service account namespace [my*team] must begin with a letter or digit"),
                containsString("service account service name [worker*] must begin with a letter or digit")
            )
        );
    }

    public void testAccountIdNamesThePathItWasBuiltFrom() {
        assertThat(new DeleteUserManagedServiceAccountRequest("my-team", "worker").getAccountId().asPrincipal(), equalTo("my-team/worker"));
    }

    private static DeleteUserManagedServiceAccountRequest newRequest(
        String namespace,
        String serviceName,
        WriteRequest.RefreshPolicy refreshPolicy,
        boolean force
    ) {
        final DeleteUserManagedServiceAccountRequest request = new DeleteUserManagedServiceAccountRequest(namespace, serviceName);
        request.setRefreshPolicy(refreshPolicy);
        request.setForce(force);
        return request;
    }
}
