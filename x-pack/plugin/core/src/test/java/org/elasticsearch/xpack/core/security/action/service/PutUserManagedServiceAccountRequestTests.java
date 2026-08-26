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
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutUserManagedServiceAccountRequestTests extends AbstractWireSerializingTestCase<PutUserManagedServiceAccountRequest> {

    @Override
    protected Writeable.Reader<PutUserManagedServiceAccountRequest> instanceReader() {
        return PutUserManagedServiceAccountRequest::new;
    }

    @Override
    protected PutUserManagedServiceAccountRequest createTestInstance() {
        return newRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomRoles(),
            randomBoolean(),
            randomFrom(WriteRequest.RefreshPolicy.values())
        );
    }

    @Override
    protected PutUserManagedServiceAccountRequest mutateInstance(PutUserManagedServiceAccountRequest instance) {
        return switch (between(0, 4)) {
            case 0 -> newRequest(
                randomValueOtherThan(instance.getNamespace(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.getServiceName(),
                instance.getRoles(),
                instance.isEnabled(),
                instance.getRefreshPolicy()
            );
            case 1 -> newRequest(
                instance.getNamespace(),
                randomValueOtherThan(instance.getServiceName(), () -> randomAlphaOfLengthBetween(3, 8)),
                instance.getRoles(),
                instance.isEnabled(),
                instance.getRefreshPolicy()
            );
            case 2 -> newRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                randomValueOtherThan(instance.getRoles(), PutUserManagedServiceAccountRequestTests::randomRoles),
                instance.isEnabled(),
                instance.getRefreshPolicy()
            );
            case 3 -> newRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                instance.getRoles(),
                instance.isEnabled() == false,
                instance.getRefreshPolicy()
            );
            case 4 -> newRequest(
                instance.getNamespace(),
                instance.getServiceName(),
                instance.getRoles(),
                instance.isEnabled(),
                randomValueOtherThan(instance.getRefreshPolicy(), () -> randomFrom(WriteRequest.RefreshPolicy.values()))
            );
            default -> throw new AssertionError("between(0, 4) returned something outside its own bounds");
        };
    }

    public void testParseTakesTheAccountFromThePathAndTheRestFromTheBody() throws IOException {
        final PutUserManagedServiceAccountRequest request = parse("my-team", "worker", """
            {
              "roles": ["role-a", "role-b"],
              "enabled": false
            }
            """);
        assertThat(request.getNamespace(), equalTo("my-team"));
        assertThat(request.getServiceName(), equalTo("worker"));
        assertThat(request.getAccountId().asPrincipal(), equalTo("my-team/worker"));
        assertThat(request.getRoles(), equalTo(List.of("role-a", "role-b")));
        assertThat(request.isEnabled(), is(false));
        assertThat(request.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.WAIT_UNTIL));
    }

    /**
     * A write replaces the account wholesale, so this default applies to every write and not only to the first: an
     * account disabled earlier comes back enabled when it is written again without the field.
     */
    public void testParseDefaultsToEnabled() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"]}
            """).isEnabled(), is(true));
    }

    public void testParseRequiresRoles() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parse("my-team", "worker", """
            {"enabled": true}
            """));
        assertThat(e.getMessage(), equalTo("Required [roles]"));
    }

    /**
     * The account a request is about comes from the path, so a body naming one too would be two sources of truth.
     */
    public void testParseRejectsUnknownFields() {
        final XContentParseException e = expectThrows(XContentParseException.class, () -> parse("my-team", "worker", """
            {"roles": ["role-a"], "namespace": "other-team"}
            """));
        assertThat(e.getMessage(), containsString("unknown field [namespace]"));
    }

    public void testAnAccountThatCouldExistIsAccepted() {
        assertThat(newRequest("my-team", "worker", List.of("role-a"), randomBoolean()).validate(), nullValue());
        // An account with no roles can authenticate and do nothing, which is a state an admin is allowed to ask for.
        assertThat(newRequest("my-team", "worker", List.of(), randomBoolean()).validate(), nullValue());
    }

    public void testTheReservedNamespaceIsRejectedInAnyCase() {
        for (String namespace : new String[] { "elastic", "ELASTIC", "Elastic" }) {
            final ActionRequestValidationException e = newRequest(namespace, "worker", List.of("role-a"), randomBoolean()).validate();
            assertThat("namespace [" + namespace + "] should be reserved", e, notNullValue());
            assertThat(e.validationErrors(), contains("the [elastic] namespace is reserved for built-in service accounts"));
        }
    }

    public void testEveryProblemWithTheRequestIsReportedAtOnce() {
        final ActionRequestValidationException e = newRequest("my*team", "worker*", List.of(" role-a", "role-b"), randomBoolean())
            .validate();
        assertThat(e, notNullValue());
        assertThat(
            e.validationErrors(),
            contains(
                containsString("service account namespace [my*team] must begin with a letter or digit"),
                containsString("service account service name [worker*] must begin with a letter or digit"),
                containsString("Role names must be at least")
            )
        );
    }

    private PutUserManagedServiceAccountRequest parse(String namespace, String serviceName, String body) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, body)) {
            return PutUserManagedServiceAccountRequest.parse(namespace, serviceName, parser);
        }
    }

    private static List<String> randomRoles() {
        return randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8));
    }

    private static PutUserManagedServiceAccountRequest newRequest(
        String namespace,
        String serviceName,
        List<String> roles,
        boolean enabled
    ) {
        return new PutUserManagedServiceAccountRequest(namespace, serviceName, roles, enabled);
    }

    private static PutUserManagedServiceAccountRequest newRequest(
        String namespace,
        String serviceName,
        List<String> roles,
        boolean enabled,
        WriteRequest.RefreshPolicy refreshPolicy
    ) {
        final PutUserManagedServiceAccountRequest request = newRequest(namespace, serviceName, roles, enabled);
        request.setRefreshPolicy(refreshPolicy);
        return request;
    }
}
