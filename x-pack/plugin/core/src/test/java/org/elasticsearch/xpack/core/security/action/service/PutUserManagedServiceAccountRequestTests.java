/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

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
        return new PutUserManagedServiceAccountRequest(
            randomAlphaOfLengthBetween(3, 8),
            randomAlphaOfLengthBetween(3, 8),
            randomRoles(),
            randomBoolean(),
            randomDescription(),
            randomFullName(),
            randomInitials(),
            randomFrom(WriteRequest.RefreshPolicy.values())
        );
    }

    @Override
    protected PutUserManagedServiceAccountRequest mutateInstance(PutUserManagedServiceAccountRequest instance) {
        String namespace = instance.getNamespace();
        String serviceName = instance.getServiceName();
        List<String> roles = instance.getRoles();
        boolean enabled = instance.isEnabled();
        String description = instance.getDescription();
        String fullName = instance.getFullName();
        String initials = instance.getInitials();
        WriteRequest.RefreshPolicy refreshPolicy = instance.getRefreshPolicy();
        switch (between(0, 7)) {
            case 0 -> namespace = randomValueOtherThan(namespace, () -> randomAlphaOfLengthBetween(3, 8));
            case 1 -> serviceName = randomValueOtherThan(serviceName, () -> randomAlphaOfLengthBetween(3, 8));
            case 2 -> roles = randomValueOtherThan(roles, PutUserManagedServiceAccountRequestTests::randomRoles);
            case 3 -> enabled = enabled == false;
            case 4 -> description = randomValueOtherThan(description, PutUserManagedServiceAccountRequestTests::randomDescription);
            case 5 -> fullName = randomValueOtherThan(fullName, PutUserManagedServiceAccountRequestTests::randomFullName);
            case 6 -> initials = randomValueOtherThan(initials, PutUserManagedServiceAccountRequestTests::randomInitials);
            case 7 -> refreshPolicy = randomValueOtherThan(refreshPolicy, () -> randomFrom(WriteRequest.RefreshPolicy.values()));
            default -> throw new AssertionError("between(0, 7) returned something outside its own bounds");
        }
        return new PutUserManagedServiceAccountRequest(
            namespace,
            serviceName,
            roles,
            enabled,
            description,
            fullName,
            initials,
            refreshPolicy
        );
    }

    public void testParseTakesTheAccountFromThePathAndTheRestFromTheBody() throws IOException {
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());
        final PutUserManagedServiceAccountRequest request = parse("my-team", "worker", refreshPolicy, """
            {
              "roles": ["role-a", "role-b"],
              "enabled": false,
              "description": "Deploys my-app",
              "full_name": "AlertZero",
              "initials": "AZ"
            }
            """);
        assertThat(request.getNamespace(), equalTo("my-team"));
        assertThat(request.getServiceName(), equalTo("worker"));
        assertThat(request.getAccountId().asPrincipal(), equalTo("my-team/worker"));
        assertThat(request.getRoles(), equalTo(List.of("role-a", "role-b")));
        assertThat(request.isEnabled(), is(false));
        assertThat(request.getDescription(), equalTo("Deploys my-app"));
        assertThat(request.getFullName(), equalTo("AlertZero"));
        assertThat(request.getInitials(), equalTo("AZ"));
        assertThat(request.getRefreshPolicy(), equalTo(refreshPolicy));
    }

    public void testRefreshPolicyDefaultsToWaitUntil() {
        final PutUserManagedServiceAccountRequest request = new PutUserManagedServiceAccountRequest(
            "my-team",
            "worker",
            List.of("role-a"),
            randomBoolean(),
            randomDescription(),
            randomFullName(),
            randomInitials()
        );
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

    /**
     * Leaving the field out and sending an explicit null both mean "no description", for the same reason as
     * {@link #testParseDefaultsToEnabled}: a write replaces the account wholesale, so there is nothing to keep.
     */
    public void testParseTreatsAMissingOrNullDescriptionAsNone() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"]}
            """).getDescription(), nullValue());
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"], "description": null}
            """).getDescription(), nullValue());
        // An empty description is a description, kept apart from having none so that a caller who sent one gets it back.
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"], "description": ""}
            """).getDescription(), equalTo(""));
    }

    public void testParseRejectsADescriptionThatIsNotAString() {
        final XContentParseException e = expectThrows(XContentParseException.class, () -> parse("my-team", "worker", """
            {"roles": ["role-a"], "description": ["one", "two"]}
            """));
        assertThat(e.getMessage(), containsString("description doesn't support values of type: START_ARRAY"));
    }

    public void testParseTreatsAMissingFullNameAsNone() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"]}
            """).getFullName(), nullValue());
    }

    /**
     * An explicit null is the same as leaving the field out, as with the description, so that a client that always
     * sends every field can clear it.
     */
    public void testParseTreatsANullFullNameAsNone() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"], "full_name": null}
            """).getFullName(), nullValue());
    }

    public void testParseRejectsAFullNameThatIsNotAString() {
        final XContentParseException e = expectThrows(XContentParseException.class, () -> parse("my-team", "worker", """
            {"roles": ["role-a"], "full_name": {"first": "Alert"}}
            """));
        assertThat(e.getMessage(), containsString("full_name doesn't support values of type: START_OBJECT"));
    }

    public void testParseTreatsMissingInitialsAsNone() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"]}
            """).getInitials(), nullValue());
    }

    public void testParseTreatsNullInitialsAsNone() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"], "initials": null}
            """).getInitials(), nullValue());
    }

    public void testParseKeepsInitialsExactlyAsGiven() throws IOException {
        assertThat(parse("my-team", "worker", """
            {"roles": ["role-a"], "initials": "aZ"}
            """).getInitials(), equalTo("aZ"));
    }

    public void testParseRejectsInitialsThatAreNotAString() {
        final XContentParseException e = expectThrows(XContentParseException.class, () -> parse("my-team", "worker", """
            {"roles": ["role-a"], "initials": ["A", "Z"]}
            """));
        assertThat(e.getMessage(), containsString("initials doesn't support values of type: START_ARRAY"));
    }

    /**
     * A node that predates the description is sent the request without it rather than being refused the request, so
     * that the account is still written there, only without its description.
     */
    public void testTheDescriptionIsDroppedForNodesThatDoNotKnowIt() throws IOException {
        final PutUserManagedServiceAccountRequest request = new PutUserManagedServiceAccountRequest(
            "my-team",
            "worker",
            randomRoles(),
            randomBoolean(),
            randomAlphaOfLengthBetween(1, 20),
            null,
            null,
            randomFrom(WriteRequest.RefreshPolicy.values())
        );
        final TransportVersion beforeDescription = TransportVersionUtils.getPreviousVersion(
            ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_DESCRIPTION
        );
        assertThat(
            copyInstance(request, beforeDescription),
            equalTo(
                new PutUserManagedServiceAccountRequest(
                    request.getNamespace(),
                    request.getServiceName(),
                    request.getRoles(),
                    request.isEnabled(),
                    null,
                    null,
                    null,
                    request.getRefreshPolicy()
                )
            )
        );
    }

    /**
     * A node that predates the full name and initials is sent the request without them, and keeps the fields that
     * came before them.
     */
    public void testTheFullNameAndInitialsAreDroppedForNodesThatDoNotKnowThem() throws IOException {
        final PutUserManagedServiceAccountRequest request = new PutUserManagedServiceAccountRequest(
            "my-team",
            "worker",
            List.of("role-a"),
            false,
            "Triages alerts",
            "AlertZero",
            "AZ",
            WriteRequest.RefreshPolicy.IMMEDIATE
        );
        final TransportVersion beforeFullName = TransportVersionUtils.getPreviousVersion(
            ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_FULL_NAME
        );
        assertThat(
            copyInstance(request, beforeFullName),
            equalTo(
                new PutUserManagedServiceAccountRequest(
                    "my-team",
                    "worker",
                    List.of("role-a"),
                    false,
                    "Triages alerts",
                    null,
                    null,
                    WriteRequest.RefreshPolicy.IMMEDIATE
                )
            )
        );
    }

    public void testTheFullNameAndInitialsAreKeptForNodesThatKnowThem() throws IOException {
        final PutUserManagedServiceAccountRequest request = new PutUserManagedServiceAccountRequest(
            "my-team",
            "worker",
            List.of("role-a"),
            true,
            null,
            "AlertZero",
            "AZ",
            WriteRequest.RefreshPolicy.IMMEDIATE
        );
        assertThat(copyInstance(request, ServiceAccountInfo.USER_MANAGED_SERVICE_ACCOUNT_FULL_NAME), equalTo(request));
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
        assertThat(
            new PutUserManagedServiceAccountRequest(
                "my-team",
                "worker",
                List.of("role-a"),
                randomBoolean(),
                randomDescription(),
                randomFullName(),
                randomInitials()
            ).validate(),
            nullValue()
        );
        // An account with no roles can authenticate and do nothing, which is a state an admin is allowed to ask for.
        assertThat(
            new PutUserManagedServiceAccountRequest("my-team", "worker", List.of(), randomBoolean(), randomDescription(), null, null)
                .validate(),
            nullValue()
        );
    }

    public void testTheReservedNamespaceIsRejectedInAnyCase() {
        for (String namespace : new String[] { "elastic", "ELASTIC", "Elastic" }) {
            final ActionRequestValidationException e = new PutUserManagedServiceAccountRequest(
                namespace,
                "worker",
                List.of("role-a"),
                randomBoolean(),
                randomDescription(),
                null,
                null
            ).validate();
            assertThat("namespace [" + namespace + "] should be reserved", e, notNullValue());
            assertThat(e.validationErrors(), contains("the [elastic] namespace is reserved for built-in service accounts"));
        }
    }

    public void testTooManyRolesAreRejectedIncludingDuplicates() {
        final int max = Validation.UserManagedServiceAccounts.MAX_ROLES;
        assertThat(newRequestWithRoles(roleNames(max)).validate(), nullValue());

        final List<String> tooMany = randomBoolean() ? roleNames(max + 1) : Collections.nCopies(max + 1, "role-a");
        final ActionRequestValidationException e = newRequestWithRoles(tooMany).validate();
        assertThat(e, notNullValue());
        assertThat(
            e.validationErrors(),
            contains("a service account may not have more than " + max + " roles, but [" + (max + 1) + "] were given")
        );
    }

    /**
     * The description has no meaning to Elasticsearch, so its only rule is a cap on its length.
     */
    public void testAnOverlongDescriptionIsRejected() {
        final int max = Validation.UserManagedServiceAccounts.MAX_DESCRIPTION_LENGTH;
        // The cap counts characters as a Java string does, which is how role descriptions are capped too.
        assertThat(newRequestWithDescription(randomAlphaOfLength(max)).validate(), nullValue());
        assertThat(newRequestWithDescription(randomUnicodeOfLength(max)).validate(), nullValue());

        final ActionRequestValidationException e = newRequestWithDescription(randomAlphaOfLength(max + 1)).validate();
        assertThat(e, notNullValue());
        assertThat(
            e.validationErrors(),
            contains("a service account description may not be more than " + max + " characters long, but [" + (max + 1) + "] were given")
        );
    }

    public void testAnOverlongFullNameIsRejected() {
        final int max = Validation.UserManagedServiceAccounts.MAX_FULL_NAME_LENGTH;
        assertThat(newRequestWithFullName("a".repeat(max)).validate(), nullValue());

        final ActionRequestValidationException e = newRequestWithFullName("a".repeat(max + 1)).validate();
        assertThat(e, notNullValue());
        assertThat(
            e.validationErrors(),
            contains("a service account full name may not be more than " + max + " characters long, but [" + (max + 1) + "] were given")
        );
    }

    public void testABlankFullNameIsAccepted() {
        assertThat(newRequestWithFullName("  ").validate(), nullValue());
    }

    public void testInitialsOfTwoCharactersAreAccepted() {
        assertThat(newRequestWithInitials("AZ").validate(), nullValue());
    }

    public void testInitialsOfThreeCharactersAreRejected() {
        final ActionRequestValidationException e = newRequestWithInitials("AZB").validate();
        assertThat(e, notNullValue());
        assertThat(e.validationErrors(), contains("service account initials may not be more than 2 characters long, but [3] were given"));
    }

    public void testBlankInitialsAreAccepted() {
        assertThat(newRequestWithInitials("").validate(), nullValue());
    }

    public void testEveryProblemWithTheRequestIsReportedAtOnce() {
        final ActionRequestValidationException e = new PutUserManagedServiceAccountRequest(
            "my*team",
            "worker*",
            List.of(" role-a", "role-b"),
            randomBoolean(),
            randomAlphaOfLength(Validation.UserManagedServiceAccounts.MAX_DESCRIPTION_LENGTH + 1),
            "a".repeat(Validation.UserManagedServiceAccounts.MAX_FULL_NAME_LENGTH + 1),
            "AZB"
        ).validate();
        assertThat(e, notNullValue());
        assertThat(
            e.validationErrors(),
            contains(
                containsString("service account namespace [my*team] must begin with a letter or digit"),
                containsString("service account service name [worker*] must begin with a letter or digit"),
                containsString("Role names must be at least"),
                containsString("a service account description may not be more than"),
                containsString("a service account full name may not be more than"),
                containsString("service account initials may not be more than 2 characters long")
            )
        );
    }

    private static PutUserManagedServiceAccountRequest newRequestWithRoles(List<String> roles) {
        return new PutUserManagedServiceAccountRequest("my-team", "worker", roles, randomBoolean(), randomDescription(), null, null);
    }

    private static PutUserManagedServiceAccountRequest newRequestWithDescription(String description) {
        return new PutUserManagedServiceAccountRequest("my-team", "worker", List.of("role-a"), randomBoolean(), description, null, null);
    }

    private static PutUserManagedServiceAccountRequest newRequestWithFullName(String fullName) {
        return new PutUserManagedServiceAccountRequest("my-team", "worker", List.of("role-a"), true, null, fullName, null);
    }

    private static PutUserManagedServiceAccountRequest newRequestWithInitials(String initials) {
        return new PutUserManagedServiceAccountRequest("my-team", "worker", List.of("role-a"), true, null, null, initials);
    }

    private static List<String> roleNames(int count) {
        return IntStream.range(0, count).mapToObj(i -> "role-" + i).toList();
    }

    private PutUserManagedServiceAccountRequest parse(String namespace, String serviceName, String body) throws IOException {
        return parse(namespace, serviceName, WriteRequest.RefreshPolicy.WAIT_UNTIL, body);
    }

    private PutUserManagedServiceAccountRequest parse(
        String namespace,
        String serviceName,
        WriteRequest.RefreshPolicy refreshPolicy,
        String body
    ) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, body)) {
            return PutUserManagedServiceAccountRequest.parse(namespace, serviceName, refreshPolicy, parser);
        }
    }

    private static List<String> randomRoles() {
        return randomList(0, 3, () -> randomAlphaOfLengthBetween(3, 8));
    }

    private static String randomDescription() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
    }

    private static String randomFullName() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
    }

    private static String randomInitials() {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 2);
    }
}
